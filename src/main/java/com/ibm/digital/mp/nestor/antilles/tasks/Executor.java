package com.ibm.digital.mp.nestor.antilles.tasks;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.message.Message;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.MessageCodes;
import com.ibm.digital.mp.nestor.antilles.provider.GsonUtil;
import com.ibm.digital.mp.nestor.antilles.tasks.lockmanager.Lock;
import com.ibm.digital.mp.nestor.antilles.tasks.lockmanager.LockManager;
import com.ibm.digital.mp.nestor.antilles.util.MetricsConstants;
import com.ibm.digital.mp.nestor.api.NestorApi;
import com.ibm.digital.mp.nestor.api.impl.NestorApiImpl;
import com.ibm.digital.mp.nestor.config.BluemixInfo;
import com.ibm.digital.mp.nestor.config.EnvironmentUtilities;
import com.ibm.digital.mp.nestor.config.NestorConfigurationFactory;
import com.ibm.digital.mp.nestor.db.Database;
import com.ibm.digital.mp.nestor.db.DbException;
import com.ibm.digital.mp.nestor.db.DbFactory;
import com.ibm.digital.mp.nestor.db.NoDocumentException;
import com.ibm.digital.mp.nestor.db.TaskResult;
import com.ibm.digital.mp.nestor.internal.antilles.tasks.recipes.RecipeManager;
import com.ibm.digital.mp.nestor.notifications.EmailNotification;
import com.ibm.digital.mp.nestor.notifications.NotificationUtilities;
import com.ibm.digital.mp.nestor.notifications.SlackNotification;
import com.ibm.digital.mp.nestor.notifications.WebhookNotification;
import com.ibm.digital.mp.nestor.tasks.ExecutionContext;
import com.ibm.digital.mp.nestor.tasks.FatalTaskExecutionException;
import com.ibm.digital.mp.nestor.tasks.MetricsConfiguration;
import com.ibm.digital.mp.nestor.tasks.Notification;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.tasks.Result;
import com.ibm.digital.mp.nestor.tasks.Status;
import com.ibm.digital.mp.nestor.tasks.Task;
import com.ibm.digital.mp.nestor.tasks.TaskExecutionException;
import com.ibm.digital.mp.nestor.tasks.recipes.Recipe;
import com.ibm.digital.mp.nestor.tasks.recipes.util.DateUtilities;
import com.ibm.digital.mp.nestor.util.Constants;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;

public class Executor
{
    static final long MAXIMUM_DECAY_MILLIS = 21600000L;

    private EmailNotification emailNotification = new EmailNotification();
    private SlackNotification slackNotification = new SlackNotification();
    private WebhookNotification webhookNotification = new WebhookNotification();
    private NotificationUtilities notificationUtilities = new NotificationUtilities();
    private static Logger logger;
    private String instanceId;
    private static Executor taskExecutor;
    private ExecutorService executorService;
    private ExecutorService newTaskExecutorService;
    private ExecutorService failedTaskExecutorService;
    private ExecutorService scheduledTaskExecutorService;
    private ExecutorService runningTaskExecutorService;
    private ExecutorService pollingTaskExecutorService;
    private ExecutorService queuedTaskExecutorService;
    private int queuedTaskLifeSpan;

    private static Set<String> taskList = new HashSet<>();

    private static final String DEFERRAL_INDEX = "ssmProvisioning/indexForDeferral";

    Executor(String instanceId)
    {
        // No instantiation

        // App instance id
        this.instanceId = instanceId;
        this.queuedTaskLifeSpan = NestorConfigurationFactory.getInstance().getNestorConfiguration().queuedTaskLifeSpan();

        // Logger
        logger = LogManager.getLogger(Executor.class.getName() + " [" + this.instanceId + "]");
        newTaskExecutorService = Executors
                .newFixedThreadPool(NestorConfigurationFactory.getInstance().getNestorConfiguration().newTaskThreadPoolSize());
        failedTaskExecutorService = Executors
                .newFixedThreadPool(NestorConfigurationFactory.getInstance().getNestorConfiguration().failedTaskThreadPoolSize());
        scheduledTaskExecutorService = Executors
                .newFixedThreadPool(NestorConfigurationFactory.getInstance().getNestorConfiguration().repeatedTaskThreadPoolSize());
        runningTaskExecutorService = Executors
                .newFixedThreadPool(NestorConfigurationFactory.getInstance().getNestorConfiguration().repeatedTaskThreadPoolSize());
        pollingTaskExecutorService = Executors
                .newFixedThreadPool(NestorConfigurationFactory.getInstance().getNestorConfiguration().pollingTaskThreadPoolSize());
        queuedTaskExecutorService = Executors
                .newFixedThreadPool(NestorConfigurationFactory.getInstance().getNestorConfiguration().queuedTaskThreadPoolSize());
        executorService = Executors
                .newFixedThreadPool(NestorConfigurationFactory.getInstance().getNestorConfiguration().taskThreadPoolSize());

    }

    /**
     * Get instance.
     * 
     * @return TaskExecutor
     */
    public static Executor getInstance()
    {
        if (taskExecutor == null)
        {
            taskExecutor = new Executor(BluemixInfo.getAppInstance());
        }
        return taskExecutor;
    }

    protected ExecutorService getExecutorService(String taskStatus)
    {
        ExecutorService execService = null;
        try
        {
            Status status = Status.valueOf(taskStatus);
            switch (status)
            {
                case NEW:
                    execService = newTaskExecutorService;
                    break;
                case FAILED:
                    execService = failedTaskExecutorService;
                    break;
                case SCHEDULED:
                    execService = scheduledTaskExecutorService;
                    break;
                case RUNNING:
                    execService = runningTaskExecutorService;
                    break;
                case POLLING:
                    execService = pollingTaskExecutorService;
                    break;
                case QUEUED:
                    execService = queuedTaskExecutorService;
                    break;
                default:
                    execService = executorService;
            }
        }
        catch (Exception ex)
        {
            execService = executorService;
        }
        return execService;
    }

    /**
     * Asynchronous task executer method.
     * 
     * @param taskId
     *            task id which is need to be executed
     * @param revision
     *            version of task
     */
    public void asyncExecuteTask(final String taskId, final String revision, String status)
    {
        ExecutorService execService = getExecutorService(status);

        if (taskList.contains(taskId))
        {
            // Task already being processed
            return;
        }

        logger.info(
                "Task {} [{}] with revision {} queued for execution on {} region {} instance {}. "
                        + "There are currently a total of {} orders on this instance of Antilles.",
                taskId, status, revision, execService.getClass().getName(), BluemixInfo.getRegion(), BluemixInfo.getAppInstance(),
                taskList.size());

        // Add tasks to tracker
        taskList.add(taskId);

        try
        {
            execService.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                {
                    // Use ThreadContext to log custom attributes that help traceability
                    ThreadContext.put("taskId", taskId);
                    ThreadContext.put("env", EnvironmentUtilities.getEnvironmentName());
                    ThreadContext.put("region", BluemixInfo.getRegion());
                    ThreadContext.put("appInstance", BluemixInfo.getAppInstance());
                    executeTask(taskId, revision);
                    ThreadContext.clearMap();
                    // Remove tasks from tracker
                    taskList.remove(taskId);
                    return null;
                }
            });
        }
        catch (Throwable th)
        {
            logger.error("Task {} with revision {} failed to execute because of error {}", taskId, revision, th.getMessage());
            // Remove tasks from tracker
            taskList.remove(taskId);
        }
    }

    protected void executeTask(String taskId, String revision)
    {
        // Acquire lock and if the thread has the lock go ahead and try seeing
        // if the task can be executed
        logger.debug("Acquiring lock for task [{}]", taskId);
        LockManager lockManager = getLockManager();
        Lock lock = lockManager.getLock(taskId);
        if (!lock.isAcquired())
        {
            logger.info("Failed to acquire lock for task [{}]", taskId);
            return;
        }

        // Lock acquired
        // Execute the task
        try
        {
            logger.debug("Successfully got lock for task [{}] and revision [{}]", taskId, revision);
            _executeTask(taskId, revision);
        }
        catch (Throwable ex)
        {
            // Error logging
            logger.error(ex.getMessage(), ex);
        }
        finally
        {
            // Unlock the lock
            lock.unlock();
            logger.debug("Unlocking done for task [{}]", taskId);
        }
    }

    /**
     * Task executor method.
     * 
     * @param taskId
     *            task id
     * @param revision
     *            revision
     * 
     * @throws DbException
     *             exception if no doc found
     */
    // CHECKSTYLE:OFF
    private void _executeTask(String taskId, String revision) throws DbException
    // CHECKSTYLE:ON
    {
        logger.info("Task {} with revision {} submitted for execution.", taskId, revision);

        Database db = getDatabase();

        Task task = null;

        try
        {
            task = db.getTask(taskId, revision);
            ThreadContext.put(Constants.TRANSACTION_LOG_ID, task.getTransactionId());
        }
        catch (NoDocumentException noDocEx)
        {
            // There might be a case where the task id or revision dosnt exist
            // This shouldnt really be an issue with the engine
            logger.info("No Document found with taskid {} and revision {}", taskId, revision);
            return;
        }

        // mark the task as scheduled if db status is RUNNING and preferredExecutionDate is in future
        if (Status.RUNNING == task.getStatus() && task.getPreferredExecutionDate() != null
                && (task.getPreferredExecutionDate().getTime() > System.currentTimeMillis()))
        {
            logger.info("Task with taskid {} and revision {} with status {} is scheduled to execute at [{}]. Hence marking it as scheduled",
                    taskId, revision, task.getStatus(), task.getPreferredExecutionDate());
            task.setStatus(Status.SCHEDULED);
            db.updateTask(task);
            return;
        }
        // Don't run tasks that are completed
        if (Status.COMPLETED == task.getStatus())
        {
            logger.info("Task is in [{}] state. Rejecting execution.", task.getStatus().toString());
            return;
        }

        // Do not execute task until preferred date unless they were queued due to dependencies
        if (task.getPreferredExecutionDate() != null && (task.getPreferredExecutionDate().getTime() > System.currentTimeMillis())
                && Status.QUEUED != task.getStatus())
        {
            logger.info("Task with taskid {} and revision {} is scheduled to execute at [{}].", taskId, revision,
                    task.getPreferredExecutionDate());
            return;
        }

        // Main execution logic

        // Since we got the lock on the task, we need to set task state as RUNNING
        try
        {
            task = markTaskRunning(task, db);
        }
        catch (Throwable th)
        {
            // This should never happen, but if it does it means that the locking logic messed up (SHOULD NEEVER HAPPEN)
            logger.error("Error occurred marking task with taskid {} and revision {} as running. Root Cause: {}", taskId, revision,
                    th.getMessage());
            return;
        }
        TaskResult resultByReferenceId = null;
        TaskResult resultByParentReferenceId = null;
        boolean executeParentReferenceIdCondn = true;
        if (task.isSequenceByReferenceId() && !StringUtils.isBlank(task.getReferenceId()))
        {
            resultByReferenceId = getAllTasksByReferenceId(task, task.getReferenceId());
            if (resultByReferenceId.getTotalRows() != 0)
            {
                executeParentReferenceIdCondn = false;
            }
        }
        if (executeParentReferenceIdCondn && task.isSequenceByParentReferenceId() && !StringUtils.isBlank(task.getParentReferenceId()))
        {
            resultByParentReferenceId = getAllTasksByReferenceId(task, task.getParentReferenceId());
        }
        // Check if other tasks need to run by sequence first
        boolean readyToExecute = readyToExecute(task, resultByReferenceId, resultByParentReferenceId);
        if (!readyToExecute && canMoveToDeferred(task, resultByReferenceId, resultByParentReferenceId))
        {
            task.setStatus(Status.DEFERRED);

            logger.info("Task [{}] cannot be executed and it has exceeded queued life span of [{}]. Moving to deferred. ", task.getId(),
                    queuedTaskLifeSpan);

            db.updateTask(task);
            return;
        }
        else if (!readyToExecute)
        {
            // Defer the task since its dependencies have not been satisfied
            task.setStatus(Status.QUEUED);

            // Increase the count this step was queued
            task.setStepProcessingCount(task.getStepProcessingCount() + 1);

            // Set the decayed time interval
            task.setPreferredExecutionDate(
                    DateUtilities.getDate(DateUtilities.getCurrentDateAsLong() + getDecayedWaitTime(task.getStepProcessingCount())));

            task = db.updateTask(task);

            logger.info("Task [{}] cannot be executed yet. Dependencies have not been not satisfied. Attempting again at [{}]",
                    task.getId(), task.getPreferredExecutionDate());

            return;
        }
        Profile profile = getDatabase().getProfile(task.getOwner());
        // Add the task details to the logging context
        addLoggingContext(task);

        Recipe recipe = getRecipe(task.getType(), profile);
        NestorApi nestorApi = new NestorApiImpl(profile);
        ExecutionContext executionContext = createExecutionContext(nestorApi, task, profile);

        try
        {
            // Execute the recipe
            logger.debug("Dependency satisfied and the task will be executed now", taskId);
            task = runTask(profile, recipe, task, executionContext, db);
            logger.info("Completed running task {} for now", task.getId());
        }
        catch (DbException ex)
        {
            // DB exception is typically a system error
            // needs to be dealt with
            markTaskFailed(task);
        }
        catch (Throwable throwable)
        {
            markTaskFailed(task);

            if (throwable instanceof TaskExecutionException)
            {
                task.setStatusDetails(((TaskExecutionException) throwable).getErrorCode(),
                        ((TaskExecutionException) throwable).getMessage());
            }

            task.setVault(executionContext.getVault(), EnvironmentUtilities.getSecret(profile.getId(), EnvironmentUtilities.VAULT_SECRET),
                    logger);

            String msg;

            // Retry logic - we need this to handle failures in recipes
            if (task.getFailedExecutionCount() >= profile.getExecutionConfig().getMaxRetries()
                    || (throwable instanceof FatalTaskExecutionException) || !(throwable instanceof TaskExecutionException))
            {
                Status status = Status.NEEDS_ATTENTION;
                if (throwable instanceof TaskExecutionException)
                {
                    status = Status.valueOf(((TaskExecutionException) throwable).getTargetStatus().toString());
                }

                task.setStatus(status);
                msg = MessageCodes.formatMessage(logger,
                        "The task has run out of retries after failures or threw a Fatal/Validation exception. "
                                + "Setting it to [{}] state. Exception: [{}]",
                        status, throwable.getMessage());
            }
            else
            {
                msg = MessageCodes.formatMessage(logger,
                        "Recipe threw an exception while processing task and setting retry to [{}]." + " Exception: [{}]",
                        task.getPreferredExecutionDate(), throwable.getMessage());
            }

            // Add the task details to the logging context
            addLoggingContext(task);
            logger.warn(msg);
        }

        task = db.updateTask(task);

        sendNotifications(task, profile.getNotification());

        // Check if there are related tasks
        if (Status.COMPLETED.equals(task.getStatus()) && task.getReferenceId() != null)
        {
            // We found a reference id, so lets try and run those tasks
            triggerTasksByReferenceId(task);
        }
    }

    /**
     * Return lock manager object which we can use to obtain distributed lock on Nestor task.
     * 
     * @return Lock manager instance
     */
    protected LockManager getLockManager()
    {
        return new LockManager();
    }

    /**
     * Change the status of given Nestor task as RUNNING.
     * 
     * @param task
     *            Task of which status change should be done
     * @param db
     *            Database instance object
     * @return Updated Nestor task with status as RUNNING
     * @throws DbException
     *             If any error occurs while updating the task status
     */
    protected Task markTaskRunning(Task task, Database db) throws DbException
    {
        ThreadContext.put(Constants.OLD_TASK_STATUS, task.getStatus().name());
        ThreadContext.put(Constants.OLD_PREFERRED_EXECUTION_DATE,
                task.getPreferredExecutionDate() != null ? String.valueOf(task.getPreferredExecutionDate().getTime()) : null);
        task.setStatus(Status.RUNNING);
        task.clearStatusDetails();
        task.setPreferredExecutionDate(null);
        return db.updateTask(task);
    }

    protected boolean readyToExecute(Task task, TaskResult resultByReferenceId, TaskResult resultByParentReferenceId) throws DbException
    {
        boolean readyToExecute = true;
        if (resultByReferenceId != null && resultByReferenceId.getTotalRows() != 0)
        {
            readyToExecute = false;
            logger.info("The task {} is sequenced by referneceId {}, {} previous tasks have not completed yet.", task.getId(),
                    task.getReferenceId(), resultByReferenceId.getTotalRows());
        }
        if (readyToExecute && resultByParentReferenceId != null && resultByParentReferenceId.getTotalRows() != 0)
        {
            readyToExecute = false;
            logger.info("The task {} is sequenced by parentReferneceId {}, {} previous tasks have not completed yet.", task.getId(),
                    task.getParentReferenceId(), resultByParentReferenceId.getTotalRows());
        }
        return readyToExecute;
    }

    protected TaskResult getAllTasksByReferenceId(Task task, String referenceId) throws DbException
    {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("referenceId", referenceId);
        // Add Created Date Range Query
        queryParams.put("createdDate", ExecutorUtil.createRange(0, task.getCreatedDate().getTime() - 1));
        // Add Status query
        queryParams.put("status", ExecutorUtil.constructCommaSeparatedValue(ExecutorUtil.getIncompleteStatuses()));

        return getDatabase().getResultByIndexQuery(DEFERRAL_INDEX, ExecutorUtil.generateLuceneQuery(queryParams),
                "[\"-modifiedDate<number>\"]", false, 10, null);
    }

    protected boolean canMoveToDeferred(Task task, TaskResult resultByReferenceId, TaskResult resultByParentReferenceId)
    {
        boolean canMoveToDeferred = false;
        if (isTaskExceedingQueuedLifeSpan(task))
        {
            canMoveToDeferred = isParentTaskBlockedOrNa(resultByReferenceId);
            canMoveToDeferred = canMoveToDeferred ? canMoveToDeferred : isParentTaskBlockedOrNa(resultByParentReferenceId);
        }
        return canMoveToDeferred;

    }

    private boolean isParentTaskBlockedOrNa(TaskResult result)
    {
        boolean isParentTaskBlockedOrNa = false;
        if (result != null && result.getTotalRows() != 0)
        {
            for (JsonObject taskJson : result.getTaskJsonList())
            {
                String status = taskJson.get("status").getAsString();
                if (Status.NEEDS_ATTENTION.toString().equals(status) || Status.BLOCKED.toString().equals(status)
                        || Status.POLLING.toString().equals(status) || Status.DEFERRED.toString().equals(status))
                {
                    isParentTaskBlockedOrNa = true;
                    break;
                }
            }
        }

        return isParentTaskBlockedOrNa;
    }

    private boolean isTaskExceedingQueuedLifeSpan(Task task)
    {
        boolean isTaskExceedingQueuedLifeSpan = false;
        Date createdDate = task.getCreatedDate();
        Date currentDate = DateUtilities.getCurrentDate();
        long diff = currentDate.getTime() - createdDate.getTime();
        long diffDays = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
        if (diffDays > queuedTaskLifeSpan)
        {
            isTaskExceedingQueuedLifeSpan = true;
        }
        return isTaskExceedingQueuedLifeSpan;
    }

    private void sendNotifications(Task task, List<Notification> notifications)
    {
        for (Notification notification : notifications)
        {
            if (notification.getStatus().equals(task.getStatus()))
            {
                String subject = "Task :" + task.getId() + " is in " + StringUtils.capitalize(task.getStatus().name().toLowerCase())
                        + " state.";
                if (notification.getEmailAddress() != null && !notification.getEmailAddress().isEmpty())
                {
                    String emailTemplate = "/mail/" + task.getStatus().name().toLowerCase() + "_mail_template.html";
                    String body = notificationUtilities.getMessage(task, emailTemplate);
                    emailNotification.sendAlert(subject, body, notification.getEmailAddress());
                }
                if (notification.getSlack() != null && !notification.getSlack().isEmpty())
                {
                    slackNotification.postSlackMessage(task, notification.getSlack());
                }
                if (notification.getWebhook() != null)
                {
                    webhookNotification.postNotificationOnWebhook(task, notification.getWebhook());
                }

            }
        }
    }

    private void addLoggingContext(Task task)
    {
        ThreadContext.put("referenceId", task.getReferenceId());
        ThreadContext.put("type", task.getType());
        ThreadContext.put("status", task.getStatus().toString());
        ThreadContext.put("step", task.getStep());
        ThreadContext.put("owner", task.getOwner());
    }

    protected Database getDatabase()
    {
        return DbFactory.getDatabase();
    }

    protected Recipe getRecipe(String taskType, Profile profile)
    {
        return RecipeManager.getInstance().getRecipe(taskType, profile);
    }

    /*
     * Run a task and update its results
     */
    protected Task runTask(Profile profile, Recipe recipe, Task task, ExecutionContext executionContext, Database db) throws Throwable
    {
        while (Status.RUNNING == task.getStatus())
        {
            logger.info("Running step {} for task {}", task.getStep(), task.getId());
            task.setLastExecutionDate(DateUtilities.getCurrentDate());
            task.setExecutionCount(task.getExecutionCount() + 1);

            String previousStep = task.getStep();

            task.createMetric(task.getStep(), task.getLastExecutionDate().getTime());

            Result result = runStep(profile, task, executionContext, recipe);

            task.updateMetric(task.getStep(), DateUtilities.getCurrentDateAsLong(), task.getExecutionCount(), task.getStepProcessingCount(),
                    task.getFailedExecutionCount(), result.getNextStep());

            task.setStatus(Status.valueOf(result.getStatus().toString()));
            task.setVault(result.getVault(), EnvironmentUtilities.getSecret(profile.getId(), EnvironmentUtilities.VAULT_SECRET), logger);
            task.setStep(result.getNextStep());
            executionContext.setStepId(result.getNextStep());
            executionContext.setVault(result.getVault());

            // if the preferred date is set then the recipe wanted to schedule the task for execution later
            // Has a preferred execution time - mark as scheduled
            if (result.getNextExecutionDate() != null && task.getStatus() != Status.COMPLETED)
            {
                task.setStatus(Status.SCHEDULED);
                task.setPreferredExecutionDate(result.getNextExecutionDate());
            }

            // mark task for archiver or deletion
            if (result.isMarkForDeletion())
            {
                task.setMarkForDeletion(Boolean.TRUE);
            }
            else if (result.isReadyToArchive())
            {
                task.setReadyToArchive(Boolean.TRUE);
            }

            if (task.getStatus() != Status.COMPLETED)
            {

                // if the preferred date is set then the recipe wanted to schedule the task for execution later
                // Has a preferred execution time - mark as scheduled
                if (result.getNextExecutionDate() != null)
                {
                    task.setStatus(Status.SCHEDULED);
                    task.setPreferredExecutionDate(result.getNextExecutionDate());
                }

                if (previousStep.equals(result.getNextStep()))
                {
                    task.setStatus(Status.POLLING);

                    // Increase the count this step is polling
                    task.setStepProcessingCount(task.getStepProcessingCount() + 1);

                    // Set the preferred time if available otherwise decay
                    if (result.getNextExecutionDate() != null)
                    {
                        task.setPreferredExecutionDate(result.getNextExecutionDate());
                    }
                    else
                    {
                        // Set the decayed time interval
                        task.setPreferredExecutionDate(DateUtilities
                                .getDate(DateUtilities.getCurrentDateAsLong() + getDecayedWaitTime(task.getStepProcessingCount())));
                    }
                }
                else
                {
                    // Reset the count
                    task.setStepProcessingCount(0);
                }
                logger.info("Task [{}] set to [{}] and to retry on [{}]", task.getId(), task.getStatus().toString(),
                        task.getPreferredExecutionDate());
            }

            // No failures, reset failure count
            task.setFailedExecutionCount(0);

            // Add the task details to the logging context
            addLoggingContext(task);

            // Update task details in the db - required for check points
            task = db.updateTask(task);
        }
        return task;
    }

    /**
     * Runs a single step for a recipe.
     * 
     * @param profile
     *            The current profile
     * @param task
     *            The current task
     * @param executionContext
     *            the current execution context
     * @param recipe
     *            The recipe to run
     * @return A result of execution
     * @throws Throwable
     *             In case of an execution error
     */
    @Trace(dispatcher = true)
    protected Result runStep(Profile profile, Task task, ExecutionContext executionContext, Recipe recipe) throws Throwable
    {
        // Setup metrics tracking
        String category = createTransactionCategory(profile);
        String name = createTransactionName(task, executionContext, profile.getMetricsConfiguration());
        NewRelic.setTransactionName(category, name);

        NewRelic.addCustomParameter(MetricsConstants.CUSTOM_PARAM_TASK_TYPE, task.getType());
        NewRelic.addCustomParameter(MetricsConstants.CUSTOM_PARAM_RECIPE_CLASS, recipe.getClass().getName());
        NewRelic.addCustomParameter(MetricsConstants.CUSTOM_PARAM_STEP_ID, executionContext.getStepId());
        NewRelic.addCustomParameter(MetricsConstants.CUSTOM_PARAM_TASK_ID, task.getId());
        NewRelic.addCustomParameter(MetricsConstants.CUSTOM_PARAM_REFERENCE_ID, task.getReferenceId());
        NewRelic.addCustomParameter(MetricsConstants.CUSTOM_PARAM_CREATED_DATE, task.getCreatedDate().getTime());
        NewRelic.addCustomParameter(MetricsConstants.CUSTOM_PARAM_OLD_TASK_STATUS, ThreadContext.get(Constants.OLD_TASK_STATUS));
        NewRelic.addCustomParameter(MetricsConstants.CUSTOM_PARAM_OLD_PREFERRED_EXECUTION_DATE,
                ThreadContext.get(Constants.OLD_PREFERRED_EXECUTION_DATE));
        NewRelic.addCustomParameter(MetricsConstants.CUSTOM_PARAM_TASK_EXECUTION_DATETIME, DateUtilities.getCurrentDateAsLong());

        Map<String, String> customMetrics = extractCustomMetrics(task, profile);
        for (Entry<String, String> entry : customMetrics.entrySet())
        {
            NewRelic.addCustomParameter(entry.getKey(), entry.getValue());
        }

        Result result = null;
        try
        {
            result = recipe.execute(executionContext);
            if (result == null)
            {
                Message msg = logger.getMessageFactory().newMessage(
                        MessageCodes.EXECUTE_RECIPE_RESULT_IS_NULL_CODE + MessageCodes.EXECUTE_RECIPE_RESULT_IS_NULL_MESSAGE,
                        task.getType(), executionContext.getStepId());
                logger.warn(msg);
                NewRelic.addCustomParameter(MetricsConstants.CUSTOM_PARAM_RESULT_STATUS, Status.FAILED.toString());
                throw new TaskExecutionException(msg.getFormattedMessage());
            }
            NewRelic.addCustomParameter(MetricsConstants.CUSTOM_PARAM_RESULT_STATUS, result.getStatus().toString());
        }
        catch (Exception ex)
        {
            // Caught to track the failure result status for the run task transaction
            NewRelic.addCustomParameter(MetricsConstants.CUSTOM_PARAM_RESULT_STATUS, Status.FAILED.toString());
            throw ex;
        }
        return result;
    }

    /*
     * Trigger the execution of tasks that are related to the current task. All tasks with the same reference id as this one are attempted.
     * All tasks with parent reference as this task's reference are attempted.
     */
    protected void triggerTasksByReferenceId(Task task) throws DbException
    {
        try
        {
            Map<String, String> taskIds = getAllTaskIdsByReferenceId(getDatabase(), task);

            for (Entry<String, String> entry : taskIds.entrySet())
            {
                asyncExecuteTask(entry.getKey(), entry.getValue(), Status.QUEUED.toString());
            }
        }
        catch (DbException ex)
        {
            logger.warn("Failed to fetch and execute tasks by reference id [{}] for task [{}].", task.getReferenceId(), task.getId());
            throw ex;
        }
    }

    /**
     * Get all task ids by referenceid.
     * 
     * @param db
     *            Database
     * @param task
     *            Task
     * @return Map
     * @throws DbException
     *             exception
     */
    public Map<String, String> getAllTaskIdsByReferenceId(Database db, Task task) throws DbException
    {
        try
        {
            Map<String, String> taskIds = db.getTaskIdsByReferenceId(task.getReferenceId());
            taskIds.putAll(db.getTaskIdsByParentReferenceId(task.getReferenceId()));

            // With eventual consistency behaviour it's possible that
            // the same task comes back in the query result and get's executed.
            // Remove the current task from the map
            if (taskIds.containsKey(task.getId()))
            {
                taskIds.remove(task.getId());
            }

            return taskIds;

        }
        catch (DbException ex)
        {
            logger.warn("Failed to fetch and execute tasks by reference id [{}] for task [{}].", task.getReferenceId(), task.getId());
            throw ex;
        }
    }

    static long getDecayedWaitTime(int retryCount)
    {
        retryCount -= 3;
        double decayFactor = Math.pow(2, retryCount) * 1000.0D;
        if (Double.isInfinite(decayFactor) || decayFactor > Long.MAX_VALUE)
        {
            return MAXIMUM_DECAY_MILLIS;
        }

        long decay = 20000L + ((long) decayFactor);

        return decay > MAXIMUM_DECAY_MILLIS ? MAXIMUM_DECAY_MILLIS : decay;
    }

    /**
     * This method creates an {@link ExecutionContext} and ensures that references to objects are not carried in.
     * 
     * @param task
     *            The task.
     * @param profile
     *            The current profile.
     * @return An {@link ExecutionContext}.
     */
    private ExecutionContext createExecutionContext(NestorApi nestorApi, Task task, Profile profile)
    {
        JsonObject additionalTaskProperties = extractTaskProperties(task, profile);

        String taskId = task.getId();
        String referenceId = task.getReferenceId();
        String stepId = task.getStep();
        int iterationCount = task.getExecutionCount();
        int retryCount = task.getFailedExecutionCount();

        JsonObject payload = GsonUtil.deepCopy(task.getPayload(), JsonObject.class);
        JsonObject vault = GsonUtil.deepCopy(
                task.getVault(EnvironmentUtilities.getSecret(profile.getId(), EnvironmentUtilities.VAULT_SECRET), logger),
                JsonObject.class);
        JsonObject configuration = GsonUtil.deepCopy(profile.getConfiguration(), JsonObject.class);
        return new ExecutionContext(nestorApi, taskId, referenceId, payload, vault, configuration, stepId, iterationCount, retryCount,
                additionalTaskProperties);
    }

    /**
     * Creates the transaction category in the <code>"< profile_id >_recipes"</code> format.
     * 
     * @param profile
     *            The current profile
     * @return The transaction category
     */
    protected String createTransactionCategory(Profile profile)
    {
        return profile.getId() + MetricsConstants.TRANSACTION_CATEGORY_SUFFIX_RECIPES;
    }

    /**
     * Creates a name for the current transaction in "<code>/< task type >/< step id ></code>" format.
     * 
     * @param task
     *            The current task
     * @param executionContext
     *            The current execution context
     * @return The transaction name
     */
    protected String createTransactionName(Task task, ExecutionContext executionContext, MetricsConfiguration metricsConfiguration)
    {
        String transactionName = "/" + task.getType() + "/" + executionContext.getStepId();
        if (!StringUtils.isBlank(metricsConfiguration.getTransactionGroup()))
        {
            transactionName = transactionName + "/" + metricsConfiguration.getTransactionGroup();
        }
        return transactionName;
    }

    /**
     * Extracts custom metrics configured in the current profile from the Task's payload. <br/>
     * <b>Note:</b> The custom metric name will be prefixed with the profile id in the format
     * <b><code>< profile id >_< metric name ></code></b>
     * 
     * @param task
     *            The current task
     * @param profile
     *            The current profile
     * @return A map of name value pairs to record as custom metrics
     */
    protected Map<String, String> extractCustomMetrics(Task task, Profile profile)
    {
        Map<String, String> metricValues = new HashMap<>();
        Configuration jsonPathConfig = Configuration.builder().build().addOptions(Option.SUPPRESS_EXCEPTIONS);
        DocumentContext jsonDocCtxt = JsonPath.parse(task.getPayload().toString(), jsonPathConfig);
        String profileId = profile.getId();
        MetricsConfiguration metricsConfiguration = profile.getMetricsConfiguration();
        for (Entry<String, String> entry : metricsConfiguration.getMetricsParameters().entrySet())
        {
            String propertyName = profileId + "_" + entry.getKey();
            try
            {
                JsonPath path = JsonPath.compile(entry.getValue());
                Object match = jsonDocCtxt.read(path);
                if (match != null)
                {
                    metricValues.put(propertyName, match.toString());
                }
            }
            catch (InvalidPathException ipEx)
            {
                metricValues.put(propertyName, entry.getValue().substring(1));
            }
            catch (Exception ex)
            {
                logger.debug("Exception occurred extracting metrics for" + " name [" + entry.getKey() + "] and JsonPath expression ["
                        + entry.getValue() + "].", ex);
            }
        }
        return metricValues;
    }

    protected JsonObject extractTaskProperties(Task task, Profile profile)
    {
        GsonBuilder gsonBuilder = new GsonBuilder();
        Gson gson = gsonBuilder.create();
        JsonObject taskJson = gson.toJsonTree(task).getAsJsonObject();
        JsonObject taskProperties = new JsonObject();
        Configuration jsonPathConfig = Configuration.builder().build().addOptions(Option.SUPPRESS_EXCEPTIONS);
        DocumentContext jsonDocCtxt = JsonPath.parse(taskJson.toString(), jsonPathConfig);
        Map<String, String> propConfig = profile.getAdditionalTaskProperties();
        if (propConfig != null)
        {
            for (Entry<String, String> entry : propConfig.entrySet())
            {
                try
                {
                    JsonPath path = JsonPath.compile(entry.getValue());
                    Object match = jsonDocCtxt.read(path);
                    if (match != null)
                    {
                        if (match instanceof Boolean)
                        {
                            taskProperties.addProperty(entry.getKey(), (Boolean) match);
                        }
                        else if (match instanceof Number)
                        {
                            taskProperties.addProperty(entry.getKey(), (Number) match);
                        }
                        else if (match instanceof String)
                        {
                            taskProperties.addProperty(entry.getKey(), (String) match);
                        }
                        else
                        {
                            taskProperties.addProperty(entry.getKey(), match.toString());
                        }
                    }
                }
                catch (InvalidPathException ipEx)
                {
                    taskProperties.addProperty(entry.getKey(), entry.getValue().substring(1));
                }
                catch (Exception ex)
                {
                    logger.error("Exception in reading additional task properties for ProfileId {}, name {} and JsonPath expression {}.",
                            profile.getId(), entry.getKey(), entry.getValue(), ex);
                }
            }
        }
        return taskProperties;
    }

    /**
     * Change the status of given Nestor task as FAILED.
     * 
     * @param task
     *            Task of which status change should be done
     * @return Updated task with status as FAILED
     */
    protected Task markTaskFailed(Task task)
    {
        task.setFailedExecutionCount(task.getFailedExecutionCount() + 1);
        task.setStatus(Status.FAILED);
        // Set the decayed time interval
        task.setPreferredExecutionDate(
                DateUtilities.getDate(DateUtilities.getCurrentDateAsLong() + getDecayedWaitTime(task.getFailedExecutionCount())));
        return task;
    }
}
