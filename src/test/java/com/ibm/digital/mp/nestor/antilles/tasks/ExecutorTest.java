package com.ibm.digital.mp.nestor.antilles.tasks;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.digital.mp.nestor.antilles.provider.GsonUtil;
import com.ibm.digital.mp.nestor.antilles.tasks.lockmanager.Lock;
import com.ibm.digital.mp.nestor.antilles.tasks.lockmanager.LockManager;
import com.ibm.digital.mp.nestor.antilles.tasks.recipes.samples.DummyRecipe;
import com.ibm.digital.mp.nestor.antilles.tasks.recipes.samples.PollingDummyRecipe;
import com.ibm.digital.mp.nestor.antilles.tasks.recipes.samples.ScheduledDummyRecipe;
import com.ibm.digital.mp.nestor.antilles.util.MetricsConstants;
import com.ibm.digital.mp.nestor.client.AntillesClient;
import com.ibm.digital.mp.nestor.config.BluemixInfo;
import com.ibm.digital.mp.nestor.config.EnvironmentUtilities;
import com.ibm.digital.mp.nestor.config.NestorConfiguration;
import com.ibm.digital.mp.nestor.config.NestorConfigurationFactory;
import com.ibm.digital.mp.nestor.db.Database;
import com.ibm.digital.mp.nestor.db.DbException;
import com.ibm.digital.mp.nestor.db.DbFactory;
import com.ibm.digital.mp.nestor.db.NoDocumentException;
import com.ibm.digital.mp.nestor.db.TaskResult;
import com.ibm.digital.mp.nestor.internal.antilles.tasks.recipes.RecipeManager;
import com.ibm.digital.mp.nestor.tasks.AllTasks;
import com.ibm.digital.mp.nestor.tasks.ExecutionConfig;
import com.ibm.digital.mp.nestor.tasks.ExecutionContext;
import com.ibm.digital.mp.nestor.tasks.FatalTaskExecutionException;
import com.ibm.digital.mp.nestor.tasks.MetricsConfiguration;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.tasks.Result;
import com.ibm.digital.mp.nestor.tasks.Status;
import com.ibm.digital.mp.nestor.tasks.Task;
import com.ibm.digital.mp.nestor.tasks.TaskExecutionException;
import com.ibm.digital.mp.nestor.tasks.TaskExecutionException.TargetStatus;
import com.ibm.digital.mp.nestor.tasks.recipes.AbstractRecipe;
import com.ibm.digital.mp.nestor.tasks.recipes.Recipe;
import com.ibm.digital.mp.nestor.tasks.recipes.util.DateUtilities;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ AntillesClient.class, EnvironmentUtilities.class, Executors.class, NestorConfiguration.class,
        NestorConfigurationFactory.class, ExecutorService.class, Executor.class, DbFactory.class, Database.class, RecipeManager.class,
        Profile.class })
@PowerMockIgnore({ "javax.management.*", "javax.crypto.*" })
public class ExecutorTest
{
    /**
     * Setup steps for all tasks.
     */
    @Before
    public void setup()
    {
        PowerMock.mockStatic(EnvironmentUtilities.class);
        EasyMock.expect(BluemixInfo.getAppInstance()).andReturn("12334").anyTimes();
        EasyMock.expect(BluemixInfo.getRegion()).andReturn("UNKNOWN").anyTimes();
        EasyMock.expect(EnvironmentUtilities.getConfigDbAccount()).andReturn("12334").anyTimes();
        EasyMock.expect(EnvironmentUtilities.getConfigDbUser()).andReturn("12334").anyTimes();
        EasyMock.expect(EnvironmentUtilities.getConfigDbPassword()).andReturn("12334").anyTimes();
        EasyMock.expect(EnvironmentUtilities.getEnvironmentName()).andReturn("12334").anyTimes();
        EasyMock.expect(EnvironmentUtilities.getSecret(EasyMock.anyString(), EasyMock.anyString())).andReturn("0123456789test12")
                .anyTimes();
        PowerMock.mockStatic(NestorConfigurationFactory.class);
        NestorConfigurationFactory factory = PowerMock.createMock(NestorConfigurationFactory.class);
        NestorConfiguration config = PowerMock.createMock(NestorConfiguration.class);
        EasyMock.expect(NestorConfigurationFactory.getInstance()).andReturn(factory).anyTimes();
        EasyMock.expect(factory.getNestorConfiguration()).andReturn(config).anyTimes();
        EasyMock.expect(config.newTaskThreadPoolSize()).andReturn(1).anyTimes();
        EasyMock.expect(config.failedTaskThreadPoolSize()).andReturn(1).anyTimes();
        EasyMock.expect(config.queuedTaskThreadPoolSize()).andReturn(1).anyTimes();
        EasyMock.expect(config.repeatedTaskThreadPoolSize()).andReturn(1).anyTimes();
        EasyMock.expect(config.pollingTaskThreadPoolSize()).andReturn(1).anyTimes();
        EasyMock.expect(config.taskThreadPoolSize()).andReturn(1).anyTimes();
        EasyMock.expect(config.queuedTaskLifeSpan()).andReturn(5).anyTimes();
        EasyMock.expect(config.logEntriesUrl()).andReturn("http://www.tst.com").anyTimes();
        PowerMock.replayAll();
    }

    @Test
    public void testCreateTransactionCategory()
    {
        String profileId = "profile1";
        GsonUtil gu = new GsonUtil();
        Profile profile = gu.fromJson("{ \"_id\": \"" + profileId + "\" }", Profile.class, false);

        String category = new Executor("test").createTransactionCategory(profile);
        Assert.assertEquals(profileId + MetricsConstants.TRANSACTION_CATEGORY_SUFFIX_RECIPES, category);
    }

    @Test
    public void testCreateTransactionName()
    {
        String taskType = "ChangeTheWorld";
        Task task = new Task();
        task.setType(taskType);

        String stepId = "step1";
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, null, null, stepId, 0, 0);

        /**
         * Test transaction group for values 1. String 2. Value with spaces surrounding (untrimmed) 3. <code>null</code> 4. Empty string 5.
         * Space
         */
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration();
        // 1
        String transactionGroup = "group1";
        metricsConfiguration.setTransactionGroup(transactionGroup);
        String name = new Executor("test").createTransactionName(task, executionContext, metricsConfiguration);
        Assert.assertEquals("/" + taskType + "/" + stepId + "/" + transactionGroup, name);

        // 2
        transactionGroup = "  group1   ";
        metricsConfiguration.setTransactionGroup(transactionGroup);
        name = new Executor("test").createTransactionName(task, executionContext, metricsConfiguration);
        Assert.assertEquals("/" + taskType + "/" + stepId + "/" + transactionGroup, name);

        // 3
        transactionGroup = null;
        metricsConfiguration.setTransactionGroup(transactionGroup);
        name = new Executor("test").createTransactionName(task, executionContext, metricsConfiguration);
        Assert.assertEquals("/" + taskType + "/" + stepId, name);

        // 4
        transactionGroup = "";
        metricsConfiguration.setTransactionGroup(transactionGroup);
        name = new Executor("test").createTransactionName(task, executionContext, metricsConfiguration);
        Assert.assertEquals("/" + taskType + "/" + stepId, name);

        // 5
        transactionGroup = "   ";
        metricsConfiguration.setTransactionGroup(transactionGroup);
        name = new Executor("test").createTransactionName(task, executionContext, metricsConfiguration);
        Assert.assertEquals("/" + taskType + "/" + stepId, name);
    }

    @Test
    public void testExecuteFutureScheduledTask()
    {
        String taskId = "123";
        LockManager lockManager = PowerMock.createMock(LockManager.class);
        Lock mockLock = PowerMock.createMock(Lock.class);
        EasyMock.expect(mockLock.unlock()).andReturn(true);
        EasyMock.expect(mockLock.isAcquired()).andReturn(true);
        EasyMock.expect(lockManager.getLock(taskId)).andReturn(mockLock);
        PowerMock.replay(lockManager, mockLock);
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected Database getDatabase()
            {
                Task futureTask = new Task();
                futureTask.setPreferredExecutionDate(new GregorianCalendar(3018, 11, 12).getTime());
                Database db = PowerMock.createMock(Database.class);
                try
                {
                    EasyMock.expect(db.getTask(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(futureTask);
                    PowerMock.replay(db);
                }
                catch (DbException ex)
                {
                    // Error
                }
                return db;
            }

            @Override
            protected LockManager getLockManager()
            {
                return lockManager;
            }
        };
        executor.executeTask(taskId, "1213");
    }

    @Test
    public void testExtractCustomMetrics()
    {
        /**
         * Test conditions. <br>
         * 1. JsonPath parses, matches and extracts - Assert literal value exists <br>
         * 2. JsonPath parses, matches and extracts - Assert JsonElement value exists after toString <br>
         * 3. JsonPath parses, no match - Assert metric missing <br>
         * 4. JsonPath is bad - Assert metric matches literal value in metrics map <br>
         * 5. JsonPath is null - Assert metric doesn't exist <br>
         * 6. JsonPath is empty string - Assert metric doesn't exist
         */

        JsonObject payload = new JsonObject();
        Map<String, String> metricsParameters = new HashMap<>();

        // 1. JsonPath parses, matches and extracts - Assert literal value exists
        metricsParameters.put("case1", "$.metadata.name");
        JsonObject metadata = new JsonObject();
        metadata.addProperty("name", "Nestor");
        payload.add("metadata", metadata);

        // 2. JsonPath parses, matches and extracts - Assert JsonElement value exists as string
        metricsParameters.put("case2", "$.metadata");

        // 3. JsonPath parses, no match - Assert metric missing
        metricsParameters.put("case3", "$.metadata.notthere");

        // 4. JsonPath is bad - Assert metric matches literal value in metrics map
        metricsParameters.put("case4", "$LiteralValue");

        // 5. JsonPath is null - Assert metric doesn't exist
        metricsParameters.put("case5", null);

        // 6. JsonPath is empty string - Assert metric doesn't exist
        metricsParameters.put("case6", "");

        Profile profile = new Profile();
        MetricsConfiguration metricsConfiguration = new MetricsConfiguration();
        metricsConfiguration.setMetricsParameters(metricsParameters);
        profile.setMetricsConfiguration(metricsConfiguration);
        Task task = new Task();
        task.setPayload(payload);

        Executor exe = new Executor("test");
        Map<String, String> customMetrics = exe.extractCustomMetrics(task, profile);

        String profileId = profile.getId();
        Assert.assertEquals("Nestor", customMetrics.get(profileId + "_" + "case1"));
        Assert.assertEquals("{name=Nestor}", customMetrics.get(profileId + "_" + "case2"));
        Assert.assertFalse(customMetrics.containsKey(profileId + "_" + "case3"));
        Assert.assertEquals("LiteralValue", customMetrics.get(profileId + "_" + "case4"));
        Assert.assertFalse(customMetrics.containsKey(profileId + "_" + "case5"));
        Assert.assertFalse(customMetrics.containsKey(profileId + "_" + "case6"));
    }

    @Test
    public void testExtractTaskProperties()
    {
        /**
         * Test conditions. <br>
         * 1. JsonPath parses, matches and extracts - Assert literal value exists <br>
         * 2. JsonPath parses, matches and extracts - Assert JsonElement value exists after toString <br>
         * 3. JsonPath parses, no match - Assert property missing <br>
         * 4. JsonPath is bad - Assert property matches literal value in metrics map <br>
         * 5. JsonPath is null - Assert property doesn't exist <br>
         * 6. JsonPath is empty string - Assert property doesn't exist <br>
         * 7. JsonPath is parses - Assert property is of type integer <br>
         * 8. JsonPath is parses - Assert property is of type boolean <br>
         * 9. JsonPath is parses - Assert property is of type string <br>
         */

        Task task = new Task();

        Profile profile = new Profile();
        Executor exe = new Executor("test");
        JsonObject additionalProperties = exe.extractTaskProperties(task, profile);
        Assert.assertNotEquals(null, additionalProperties);

        profile.setAdditionalTaskProperties(null);
        additionalProperties = exe.extractTaskProperties(task, profile);
        Assert.assertNotEquals(null, additionalProperties);

        Map<String, String> propConfig = new HashMap<>();
        profile.setAdditionalTaskProperties(propConfig);
        additionalProperties = exe.extractTaskProperties(task, profile);
        Assert.assertNotEquals(null, additionalProperties);

        // 1. JsonPath parses, matches and extracts - Assert literal value exists
        propConfig.put("case1", "$.createdDate");
        task.setCreatedDate(new Date(1521626625491L));

        // 2. JsonPath parses, matches and extracts - Assert JsonElement value exists as string
        propConfig.put("case2", "$.payload");
        task.setPayload(new JsonObject());

        // 3. JsonPath parses, no match - Assert property missing
        propConfig.put("case3", "$.metadata.notthere");

        // 4. JsonPath is bad - Assert property matches literal value in metrics map
        propConfig.put("case4", "$LiteralValue");

        // 5. JsonPath is null - Assert property doesn't exist
        propConfig.put("case5", null);

        // 6. JsonPath is empty string - Assert property doesn't exist
        propConfig.put("case6", "");

        // 7. JsonPath is parses - Assert property is of type integer
        propConfig.put("case7", "$.executionCount");

        // 8. JsonPath is parses - Assert property is of type boolean
        propConfig.put("case8", "$.sequenceByReferenceId");

        // 8. JsonPath is parses - Assert property is of type string
        propConfig.put("case9", "$.step");

        profile.setAdditionalTaskProperties(propConfig);

        exe = new Executor("test");
        additionalProperties = exe.extractTaskProperties(task, profile);

        Assert.assertEquals(1521626625491L, additionalProperties.get("case1").getAsLong());
        Assert.assertEquals("{}", additionalProperties.get("case2").getAsString());
        Assert.assertFalse(additionalProperties.has("case3"));
        Assert.assertEquals("LiteralValue", additionalProperties.get("case4").getAsString());
        Assert.assertFalse(additionalProperties.has("case5"));
        Assert.assertFalse(additionalProperties.has("case6"));
        Assert.assertEquals(0, additionalProperties.get("case7").getAsInt());
        Assert.assertFalse(additionalProperties.get("case8").getAsBoolean());
        Assert.assertEquals("START", additionalProperties.get("case9").getAsString());
    }

    @Test
    public void testGetInstance()
    {

        PowerMock.mockStatic(Executors.class);
        ExecutorService executorService = PowerMock.createMock(ExecutorService.class);
        EasyMock.expect(Executors.newFixedThreadPool(1)).andReturn(executorService).anyTimes();
        PowerMock.replayAll(Executors.class);
        Assert.assertNotNull(Executor.getInstance());
        Assert.assertNotNull(Executor.getInstance());
    }

    @Test
    public void testGetExecutorService()
    {
        PowerMock.mockStatic(Executors.class);
        ExecutorService executorService = PowerMock.createMock(ExecutorService.class);
        EasyMock.expect(Executors.newFixedThreadPool(EasyMock.anyInt())).andReturn(executorService).anyTimes();
        PowerMock.replayAll(EnvironmentUtilities.class, Executors.class, NestorConfiguration.class, NestorConfigurationFactory.class,
                executorService);
        Executor executorInstance = Executor.getInstance();
        Assert.assertNotNull(executorInstance.getExecutorService(Status.NEW.toString()));
        Assert.assertNotNull(executorInstance.getExecutorService(Status.QUEUED.toString()));
        Assert.assertNotNull(executorInstance.getExecutorService(Status.FAILED.toString()));
        Assert.assertNotNull(executorInstance.getExecutorService(Status.POLLING.toString()));
        Assert.assertNotNull(executorInstance.getExecutorService(Status.SCHEDULED.toString()));
        Assert.assertNotNull(executorInstance.getExecutorService(Status.RUNNING.toString()));
        Assert.assertNotNull(executorInstance.getExecutorService(Status.COMPLETED.toString()));
        Assert.assertNotNull(executorInstance.getExecutorService(null));

    }

    @Test
    public void testAsyncExecuteTask()
    {
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected void executeTask(String taskId, String revision)
            {
                //
            }

        };
        executor.asyncExecuteTask("1234", "1213", Status.NEW.toString());
    }

    @Test
    public void testExecuteTaskWithNoRecipe() throws DbException
    {
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setPreferredExecutionDate(new Date());
        String taskId = "123";
        LockManager lockManager = PowerMock.createMock(LockManager.class);
        Lock mockLock = PowerMock.createMock(Lock.class);
        EasyMock.expect(mockLock.unlock()).andReturn(true);
        EasyMock.expect(lockManager.getLock(taskId)).andReturn(mockLock);
        EasyMock.expect(mockLock.isAcquired()).andReturn(true);
        Profile profile = new Profile();
        EasyMock.expect(database.getTask(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(task).anyTimes();
        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();
        EasyMock.expect(database.updateTask(EasyMock.anyObject())).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected LockManager getLockManager()
            {
                return lockManager;
            }
        };

        executor.executeTask(taskId, "123");
        Assert.assertTrue(task.getStatus() == Status.NEEDS_ATTENTION);

    }

    @Test
    public void testExecuteTaskWithDbException() throws DbException
    {
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTask(EasyMock.anyObject(), EasyMock.anyObject())).andThrow(new DbException("")).anyTimes();
        LockManager lockManager = PowerMock.createMock(LockManager.class);
        String taskId = "123";
        Lock mockLock = PowerMock.createMock(Lock.class);
        EasyMock.expect(mockLock.unlock()).andReturn(true);
        EasyMock.expect(mockLock.isAcquired()).andReturn(true);
        EasyMock.expect(lockManager.getLock(taskId)).andReturn(mockLock);
        PowerMock.replayAll(DbFactory.class, database);
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected LockManager getLockManager()
            {
                return lockManager;
            }
        };
        executor.executeTask(taskId, "1213");

    }

    @Test
    public void testExecuteTaskRefIdAndReadyToExecuteFalse() throws DbException
    {
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setSequenceByReferenceId(true);
        task.setReferenceId("12345");
        EasyMock.expect(database.getTask(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(task).anyTimes();
        AllTasks allTasks = new AllTasks();
        List<Task> taskList = new ArrayList<>();
        taskList.add(task);
        allTasks.setDocs(taskList);
        // Preparing selector
        JsonObject selectorQuery = new JsonObject();
        selectorQuery.addProperty("referenceId", task.getReferenceId());
        JsonObject creationDateQuery = new JsonObject();
        creationDateQuery.addProperty("$lt", task.getCreatedDate().getTime());
        selectorQuery.add("createdDate", creationDateQuery);
        JsonObject status = new JsonParser().parse("{\"$nin\":[\"" + Status.COMPLETED + "\",\"" + Status.CANCELLED + "\"]}")
                .getAsJsonObject();
        selectorQuery.add("status", status);

        JsonObject queryJson = new JsonObject();
        queryJson.add("selector", selectorQuery);
        TaskResult taskResult = new TaskResult(null, null, 1);
        EasyMock.expect(database.getResultByIndexQuery(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyString(),
                EasyMock.anyBoolean(), EasyMock.anyInt(), EasyMock.anyObject())).andReturn(taskResult).anyTimes();
        Task updatedTask = new Task();
        updatedTask.setId(task.getId());
        updatedTask.setRevision(task.getRevision() + "1");
        EasyMock.expect(database.updateTask(EasyMock.anyObject())).andReturn(task).anyTimes();

        LockManager lockManager = PowerMock.createMock(LockManager.class);
        String taskId = "123";
        Lock mockLock = PowerMock.createMock(Lock.class);
        EasyMock.expect(mockLock.unlock()).andReturn(true);
        EasyMock.expect(mockLock.isAcquired()).andReturn(true);
        EasyMock.expect(lockManager.getLock(taskId)).andReturn(mockLock);

        PowerMock.replayAll(DbFactory.class, database);
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

            @Override
            protected LockManager getLockManager()
            {
                return lockManager;
            }
        };
        executor.executeTask(taskId, "1213");

    }

    @Test
    public void testExecuteTaskReadyToExecuteTrueAndLockFails() throws DbException
    {
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setSequenceByReferenceId(true);
        task.setReferenceId("12345");
        EasyMock.expect(database.getTask(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(task).anyTimes();
        AllTasks allTasks = new AllTasks();
        List<Task> taskList = new ArrayList<>();
        allTasks.setDocs(taskList);
        // Preparing selector
        JsonObject selectorQuery = new JsonObject();
        selectorQuery.addProperty("referenceId", task.getReferenceId());
        JsonObject creationDateQuery = new JsonObject();
        creationDateQuery.addProperty("$lt", task.getCreatedDate().getTime());
        selectorQuery.add("createdDate", creationDateQuery);
        JsonObject statusQuery = new JsonObject();
        statusQuery.addProperty("status", Status.COMPLETED.toString());
        selectorQuery.add("$not", statusQuery);

        JsonObject queryJson = new JsonObject();
        queryJson.add("selector", selectorQuery);
        EasyMock.expect(database.getAllTasks(queryJson, 1, false, false)).andReturn(allTasks).anyTimes();
        Profile profile = new Profile();
        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();
        EasyMock.expect(database.updateTask(EasyMock.anyObject())).andThrow(new DbException("")).anyTimes();

        String taskId = "123";
        LockManager lockManager = PowerMock.createMock(LockManager.class);
        Lock mockLock = PowerMock.createMock(Lock.class);
        EasyMock.expect(lockManager.getLock(taskId)).andReturn(mockLock);
        EasyMock.expect(mockLock.isAcquired()).andReturn(false);
        EasyMock.expect(mockLock.unlock()).andReturn(true);

        PowerMock.replayAll(DbFactory.class, database);
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected LockManager getLockManager()
            {
                return lockManager;
            }
        };
        executor.executeTask(taskId, "1213");

    }

    @Test
    public void testExecuteTaskWithRecipe() throws Exception
    {
        PowerMock.mockStatic(DbFactory.class);
        AntillesClient ac = PowerMock.createNiceMock(AntillesClient.class);
        PowerMock.expectNew(AntillesClient.class).andReturn(ac).anyTimes();

        PowerMock.mockStatic(RecipeManager.class);
        RecipeManager recipeManager = PowerMock.createMock(RecipeManager.class);
        EasyMock.expect(RecipeManager.getInstance()).andReturn(recipeManager).anyTimes();
        Profile profile = new Profile();
        DummyRecipe recipe = new DummyRecipe();
        EasyMock.expect(recipeManager.getRecipe(null, profile)).andReturn(recipe).anyTimes();
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setReferenceId("12345");
        EasyMock.expect(database.getTask(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(task).anyTimes();

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();
        Map<String, String> taskMap = new HashMap<>();
        taskMap.put("1", "2");
        EasyMock.expect(database.getTaskIdsByReferenceId(EasyMock.anyObject())).andReturn(taskMap).anyTimes();
        ac.executeTask("1", "2", Status.NEW);
        EasyMock.expectLastCall().anyTimes();
        PowerMock.replay(ac);
        EasyMock.expect(database.updateTask(EasyMock.anyObject())).andReturn(task).anyTimes();

        LockManager lockManager = PowerMock.createMock(LockManager.class);
        String taskId = "123";
        Lock mockLock = PowerMock.createMock(Lock.class);
        EasyMock.expect(mockLock.unlock()).andReturn(true);
        EasyMock.expect(mockLock.isAcquired()).andReturn(true);
        EasyMock.expect(lockManager.getLock(taskId)).andReturn(mockLock);

        PowerMock.replayAll(DbFactory.class, database, RecipeManager.class);
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

            @Override
            protected LockManager getLockManager()
            {
                return lockManager;
            }
        };
        executor.executeTask(taskId, "1213");

    }

    @Test
    public void testExecuteTaskWithRecipeAndTriggerTaskThrowsException() throws Exception
    {
        PowerMock.mockStatic(DbFactory.class);
        AntillesClient ac = PowerMock.createNiceMock(AntillesClient.class);
        PowerMock.expectNew(AntillesClient.class).andReturn(ac).anyTimes();

        PowerMock.mockStatic(RecipeManager.class);
        RecipeManager recipeManager = PowerMock.createMock(RecipeManager.class);
        EasyMock.expect(RecipeManager.getInstance()).andReturn(recipeManager).anyTimes();
        Profile profile = new Profile();
        DummyRecipe recipe = new DummyRecipe();
        EasyMock.expect(recipeManager.getRecipe(null, profile)).andReturn(recipe).anyTimes();
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setSequenceByReferenceId(true);
        task.setSequenceByParentReferenceId(true);
        task.setReferenceId(null);
        task.setParentReferenceId(null);
        EasyMock.expect(database.getTask(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(task).anyTimes();

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();
        Map<String, String> taskMap = new HashMap<>();
        taskMap.put("1", "2");
        EasyMock.expect(database.getTaskIdsByReferenceId(EasyMock.anyObject())).andThrow(new DbException("")).anyTimes();
        ac.executeTask("1", "2", Status.NEW);
        EasyMock.expectLastCall().anyTimes();
        PowerMock.replay(ac);
        EasyMock.expect(database.updateTask(EasyMock.anyObject())).andReturn(task).anyTimes();
        LockManager lockManager = PowerMock.createMock(LockManager.class);
        String taskId = "123";
        Lock mockLock = PowerMock.createMock(Lock.class);
        EasyMock.expect(mockLock.unlock()).andReturn(true);
        EasyMock.expect(mockLock.isAcquired()).andReturn(true);
        EasyMock.expect(lockManager.getLock(taskId)).andReturn(mockLock);
        PowerMock.replayAll(DbFactory.class, database, RecipeManager.class);
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

            @Override
            protected LockManager getLockManager()
            {
                return lockManager;
            }
        };
        executor.executeTask(taskId, "1213");
        Assert.assertEquals(Status.FAILED, task.getStatus());
    }

    @Test
    public void testReadyToEecuter() throws Exception
    {

        Executor executor = new Executor("InstanceId")
        {
        };

        Task task = new Task();
        TaskResult resultByParentReferenceId = new TaskResult(new ArrayList<JsonObject>(), "", 0);
        task.setSequenceByReferenceId(true);
        task.setReferenceId("Test123");
        TaskResult resultByReferenceId1 = new TaskResult(new ArrayList<JsonObject>(), "", 1);
        Assert.assertTrue(!executor.readyToExecute(task, resultByReferenceId1, resultByParentReferenceId));
        TaskResult resultByReferenceId = new TaskResult(new ArrayList<JsonObject>(), "", 0);

        task.setSequenceByReferenceId(true);
        task.setReferenceId(null);
        Assert.assertTrue(executor.readyToExecute(task, resultByReferenceId, resultByParentReferenceId));

        task.setSequenceByReferenceId(false);
        task.setReferenceId(null);
        Assert.assertTrue(executor.readyToExecute(task, resultByReferenceId, resultByParentReferenceId));

        task.setSequenceByParentReferenceId(true);
        task.setParentReferenceId("Test123");
        Assert.assertTrue(!executor.readyToExecute(task, resultByReferenceId, new TaskResult(new ArrayList<JsonObject>(), "", 1)));

        task.setSequenceByParentReferenceId(true);
        task.setParentReferenceId(null);
        Assert.assertTrue(executor.readyToExecute(task, resultByReferenceId, resultByParentReferenceId));

        task.setSequenceByParentReferenceId(false);
        task.setParentReferenceId(null);
        Assert.assertTrue(executor.readyToExecute(task, resultByReferenceId, resultByParentReferenceId));
    }

    @Test
    public void testExecuteTaskWithRecipeAndTriggerTaskThrowsTaskException() throws Exception
    {
        PowerMock.mockStatic(DbFactory.class);
        AntillesClient ac = PowerMock.createNiceMock(AntillesClient.class);
        PowerMock.expectNew(AntillesClient.class).andReturn(ac).anyTimes();

        PowerMock.mockStatic(RecipeManager.class);
        RecipeManager recipeManager = PowerMock.createMock(RecipeManager.class);
        EasyMock.expect(RecipeManager.getInstance()).andReturn(recipeManager).anyTimes();
        Profile profile = new Profile();
        DummyRecipe recipe = new DummyRecipe();
        EasyMock.expect(recipeManager.getRecipe(null, profile)).andReturn(recipe).anyTimes();
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setReferenceId("12345");
        EasyMock.expect(database.getTask(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(task).anyTimes();

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();
        Map<String, String> taskMap = new HashMap<>();
        taskMap.put("1", "2");
        ac.executeTask("1", "2", Status.NEW);
        EasyMock.expectLastCall().anyTimes();
        PowerMock.replay(ac);
        EasyMock.expect(database.updateTask(EasyMock.anyObject())).andReturn(task).anyTimes();
        LockManager lockManager = PowerMock.createMock(LockManager.class);
        String taskId = "123";
        Lock mockLock = PowerMock.createMock(Lock.class);
        EasyMock.expect(mockLock.unlock()).andReturn(true);
        EasyMock.expect(lockManager.getLock(taskId)).andReturn(mockLock);
        EasyMock.expect(mockLock.isAcquired()).andReturn(true);
        PowerMock.replayAll(DbFactory.class, database, RecipeManager.class);
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected Task runTask(Profile profile, Recipe recipe, Task task, ExecutionContext executionContext, Database db)
                    throws Throwable
            {
                throw new Exception("ErrorMessage");
            }

            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

            @Override
            protected LockManager getLockManager()
            {
                return lockManager;
            }
        };

        executor.executeTask(taskId, "1213");
        Assert.assertEquals(Status.NEEDS_ATTENTION, task.getStatus());
    }

    @Test
    public void testExecuteMarkTaskRunningThrowsException() throws Exception
    {
        PowerMock.mockStatic(DbFactory.class);
        AntillesClient ac = PowerMock.createNiceMock(AntillesClient.class);
        PowerMock.expectNew(AntillesClient.class).andReturn(ac).anyTimes();

        PowerMock.mockStatic(RecipeManager.class);
        RecipeManager recipeManager = PowerMock.createMock(RecipeManager.class);
        EasyMock.expect(RecipeManager.getInstance()).andReturn(recipeManager).anyTimes();
        Profile profile = new Profile();
        DummyRecipe recipe = new DummyRecipe();
        EasyMock.expect(recipeManager.getRecipe(null, profile)).andReturn(recipe).anyTimes();
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setReferenceId("12345");
        EasyMock.expect(database.getTask(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(task).anyTimes();

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();
        Map<String, String> taskMap = new HashMap<>();
        taskMap.put("1", "2");
        ac.executeTask("1", "2", Status.NEW);
        EasyMock.expectLastCall().anyTimes();
        PowerMock.replay(ac);
        EasyMock.expect(database.updateTask(EasyMock.anyObject())).andReturn(task).anyTimes();
        LockManager lockManager = PowerMock.createMock(LockManager.class);
        String taskId = "123";
        Lock mockLock = PowerMock.createMock(Lock.class);
        EasyMock.expect(mockLock.unlock()).andReturn(true);
        EasyMock.expect(lockManager.getLock(taskId)).andReturn(mockLock);
        EasyMock.expect(mockLock.isAcquired()).andReturn(true);
        PowerMock.replayAll(DbFactory.class, database, RecipeManager.class);
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected Task runTask(Profile profile, Recipe recipe, Task task, ExecutionContext executionContext, Database db)
                    throws Throwable
            {
                throw new Exception("ErrorMessage");
            }

            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                throw new DbException("ErrorMessage");
            }

            @Override
            protected LockManager getLockManager()
            {
                return lockManager;
            }
        };

        executor.executeTask(taskId, "1213");
        Assert.assertEquals(Status.NEW, task.getStatus());
    }

    @Test
    public void testExecuteTaskWithScheduledRecipe() throws Exception
    {
        PowerMock.mockStatic(DbFactory.class);
        AntillesClient ac = PowerMock.createNiceMock(AntillesClient.class);
        PowerMock.expectNew(AntillesClient.class).andReturn(ac).anyTimes();

        PowerMock.mockStatic(RecipeManager.class);
        RecipeManager recipeManager = PowerMock.createMock(RecipeManager.class);
        EasyMock.expect(RecipeManager.getInstance()).andReturn(recipeManager).anyTimes();
        Profile profile = new Profile();
        ScheduledDummyRecipe recipe = new ScheduledDummyRecipe();
        EasyMock.expect(recipeManager.getRecipe(null, profile)).andReturn(recipe).anyTimes();
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setReferenceId("12345");
        EasyMock.expect(database.getTask(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(task).anyTimes();

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();
        Map<String, String> taskMap = new HashMap<>();
        taskMap.put("1", "2");
        EasyMock.expect(database.getTaskIdsByReferenceId(EasyMock.anyObject())).andReturn(taskMap).anyTimes();
        ac.executeTask("1", "2", Status.NEW);
        EasyMock.expectLastCall().anyTimes();
        PowerMock.replay(ac);
        EasyMock.expect(database.updateTask(EasyMock.anyObject())).andReturn(task).anyTimes();
        LockManager lockManager = PowerMock.createMock(LockManager.class);
        String taskId = "123";
        Lock mockLock = PowerMock.createMock(Lock.class);
        EasyMock.expect(mockLock.unlock()).andReturn(true);
        EasyMock.expect(lockManager.getLock(taskId)).andReturn(mockLock);
        EasyMock.expect(mockLock.isAcquired()).andReturn(true);

        PowerMock.replayAll(DbFactory.class, database, RecipeManager.class);
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

            @Override
            protected LockManager getLockManager()
            {
                return lockManager;
            }
        };
        executor.executeTask(taskId, "1213");
        Assert.assertEquals(Status.SCHEDULED, task.getStatus());
    }

    @Test
    public void testExecuteTaskWithPollingRecipe() throws Exception
    {
        PowerMock.mockStatic(DbFactory.class);
        AntillesClient ac = PowerMock.createNiceMock(AntillesClient.class);
        PowerMock.expectNew(AntillesClient.class).andReturn(ac).anyTimes();

        PowerMock.mockStatic(RecipeManager.class);
        RecipeManager recipeManager = PowerMock.createMock(RecipeManager.class);
        EasyMock.expect(RecipeManager.getInstance()).andReturn(recipeManager).anyTimes();
        Profile profile = new Profile();
        PollingDummyRecipe recipe = new PollingDummyRecipe();
        EasyMock.expect(recipeManager.getRecipe(null, profile)).andReturn(recipe).anyTimes();
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setReferenceId("12345");
        EasyMock.expect(database.getTask(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(task).anyTimes();

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();
        Map<String, String> taskMap = new HashMap<>();
        taskMap.put("1", "2");
        EasyMock.expect(database.getTaskIdsByReferenceId(EasyMock.anyObject())).andReturn(taskMap).anyTimes();
        ac.executeTask("1", "2", Status.NEW);
        EasyMock.expectLastCall().anyTimes();
        PowerMock.replay(ac);
        EasyMock.expect(database.updateTask(EasyMock.anyObject())).andReturn(task).anyTimes();
        LockManager lockManager = PowerMock.createMock(LockManager.class);
        String taskId = "123";
        Lock mockLock = PowerMock.createMock(Lock.class);
        EasyMock.expect(mockLock.unlock()).andReturn(true);
        EasyMock.expect(lockManager.getLock(taskId)).andReturn(mockLock);
        EasyMock.expect(mockLock.isAcquired()).andReturn(true);
        PowerMock.replayAll(DbFactory.class, database, RecipeManager.class);
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

            @Override
            protected LockManager getLockManager()
            {
                return lockManager;
            }
        };
        executor.executeTask(taskId, "1213");
        Assert.assertEquals(Status.POLLING, task.getStatus());
    }

    @Test
    public void testExecuteTaskWithPollingRecipeWithArchiveFlag() throws Exception
    {
        PowerMock.mockStatic(DbFactory.class);
        AntillesClient ac = PowerMock.createNiceMock(AntillesClient.class);
        PowerMock.expectNew(AntillesClient.class).andReturn(ac).anyTimes();

        PowerMock.mockStatic(RecipeManager.class);
        RecipeManager recipeManager = PowerMock.createMock(RecipeManager.class);
        EasyMock.expect(RecipeManager.getInstance()).andReturn(recipeManager).anyTimes();
        Profile profile = new Profile();
        PollingDummyRecipe recipe = new PollingDummyRecipe()
        {
            @Override
            public Result execute(ExecutionContext executionContext) throws TaskExecutionException
            {
                return new Result("END", Result.Status.RUNNING, null, true);
            }
        };
        EasyMock.expect(recipeManager.getRecipe(null, profile)).andReturn(recipe).anyTimes();
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setReferenceId("12345");
        EasyMock.expect(database.getTask(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(task).anyTimes();

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();
        Map<String, String> taskMap = new HashMap<>();
        taskMap.put("1", "2");
        EasyMock.expect(database.getTaskIdsByReferenceId(EasyMock.anyObject())).andReturn(taskMap).anyTimes();
        ac.executeTask("1", "2", Status.NEW);
        EasyMock.expectLastCall().anyTimes();
        PowerMock.replay(ac);
        EasyMock.expect(database.updateTask(EasyMock.anyObject())).andReturn(task).anyTimes();
        LockManager lockManager = PowerMock.createMock(LockManager.class);
        String taskId = "123";
        Lock mockLock = PowerMock.createMock(Lock.class);
        EasyMock.expect(mockLock.unlock()).andReturn(true);
        EasyMock.expect(lockManager.getLock(taskId)).andReturn(mockLock);
        EasyMock.expect(mockLock.isAcquired()).andReturn(true);
        PowerMock.replayAll(DbFactory.class, database, RecipeManager.class);
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

            @Override
            protected LockManager getLockManager()
            {
                return lockManager;
            }
        };
        executor.executeTask(taskId, "1213");
        Assert.assertEquals(Status.POLLING, task.getStatus());
    }

    @Test
    public void testExecuteTaskWithPollingRecipeInvalidStep() throws Exception
    {
        PowerMock.mockStatic(DbFactory.class);
        AntillesClient ac = PowerMock.createNiceMock(AntillesClient.class);
        PowerMock.expectNew(AntillesClient.class).andReturn(ac).anyTimes();

        PowerMock.mockStatic(RecipeManager.class);
        RecipeManager recipeManager = PowerMock.createMock(RecipeManager.class);
        EasyMock.expect(RecipeManager.getInstance()).andReturn(recipeManager).anyTimes();
        Profile profile = new Profile();
        PollingDummyRecipe recipe = new PollingDummyRecipe();
        EasyMock.expect(recipeManager.getRecipe(null, profile)).andReturn(recipe).anyTimes();
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setReferenceId("12345");
        task.setStep("Invalid");
        EasyMock.expect(database.getTask(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(task).anyTimes();

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();
        Map<String, String> taskMap = new HashMap<>();
        taskMap.put("1", "2");
        EasyMock.expect(database.getTaskIdsByReferenceId(EasyMock.anyObject())).andReturn(taskMap).anyTimes();
        ac.executeTask("1", "2", Status.NEW);
        EasyMock.expectLastCall().anyTimes();
        PowerMock.replay(ac);
        EasyMock.expect(database.updateTask(EasyMock.anyObject())).andReturn(task).anyTimes();
        LockManager lockManager = PowerMock.createMock(LockManager.class);
        String taskId = "123";
        Lock mockLock = PowerMock.createMock(Lock.class);
        EasyMock.expect(mockLock.unlock()).andReturn(true);
        EasyMock.expect(lockManager.getLock(taskId)).andReturn(mockLock);
        EasyMock.expect(mockLock.isAcquired()).andReturn(true);

        PowerMock.replayAll(DbFactory.class, database, RecipeManager.class);
        Executor executor = new Executor("InstanceId")
        {
            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

            @Override
            protected LockManager getLockManager()
            {
                return lockManager;
            }
        };
        executor.executeTask(taskId, "1213");
        Assert.assertEquals(Status.FAILED, task.getStatus());
    }

    @Test
    public void testTriggerTasksByReferenceId()
    {
        try
        {
            // Ensure all but the current task do not get triggered.
            final String currentTaskId = "task1";
            final String referenceId = "referenceId123";
            final Set<String> expectedTasks = new HashSet<>();
            final Map<String, String> tasksByRefId = new HashMap<>();
            tasksByRefId.put("task1", "rev1");

            tasksByRefId.put("task2", "rev2");
            expectedTasks.add("task2");

            tasksByRefId.put("task3", "rev3");
            expectedTasks.add("task3");

            tasksByRefId.put("task4", "rev4");
            expectedTasks.add("task4");

            Executor ex = new Executor("instance1")
            {
                @Override
                protected Database getDatabase()
                {
                    Database mockDb = Mockito.mock(Database.class);
                    try
                    {
                        Mockito.when(mockDb.getTaskIdsByReferenceId(Mockito.anyString())).thenReturn(tasksByRefId);
                        Mockito.when(mockDb.getTaskIdsByParentReferenceId(Mockito.anyString())).thenReturn(new HashMap<>());
                    }
                    catch (Exception ex)
                    {
                        ex.printStackTrace();
                        Assert.fail(ex.getMessage());
                    }
                    return mockDb;
                }

                @Override
                public void asyncExecuteTask(String taskId, String revision, String status)
                {
                    Assert.assertNotEquals(currentTaskId, taskId);
                    Assert.assertTrue(expectedTasks.contains(taskId));
                }
            };
            Task task = new Task();
            task.setId(currentTaskId);
            task.setReferenceId(referenceId);
            ex.triggerTasksByReferenceId(task);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void testTriggerTasksByParentReferenceId()
    {
        try
        {
            // Ensure all but the current task do not get triggered.
            final String currentTaskId = "task1";
            final String referenceId = "referenceId123";
            final Set<String> expectedTasks = new HashSet<>();
            final Map<String, String> tasksByParentRefId = new HashMap<>();

            tasksByParentRefId.put("task2", "rev2");
            expectedTasks.add("task2");

            tasksByParentRefId.put("task3", "rev3");
            expectedTasks.add("task3");

            tasksByParentRefId.put("task4", "rev4");
            expectedTasks.add("task4");

            Executor ex = new Executor("instance1")
            {
                @Override
                protected Database getDatabase()
                {
                    Database mockDb = Mockito.mock(Database.class);
                    try
                    {
                        Mockito.when(mockDb.getTaskIdsByReferenceId(Mockito.anyString())).thenReturn(new HashMap<>());
                        Mockito.when(mockDb.getTaskIdsByParentReferenceId(Mockito.anyString())).thenReturn(tasksByParentRefId);
                    }
                    catch (Exception ex)
                    {
                        ex.printStackTrace();
                        Assert.fail(ex.getMessage());
                    }
                    return mockDb;
                }

                @Override
                public void asyncExecuteTask(String taskId, String revision, String status)
                {
                    Assert.assertNotEquals(currentTaskId, taskId);
                    Assert.assertTrue(expectedTasks.contains(taskId));
                }
            };
            Task task = new Task();
            task.setId(currentTaskId);
            task.setReferenceId(referenceId);
            ex.triggerTasksByReferenceId(task);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void testTriggerByRefException()
    {
        try
        {
            // Ensure all but the current task do not get triggered.
            final String currentTaskId = "task1";
            final String referenceId = "referenceId123";

            Executor ex = new Executor("instance1")
            {
                @Override
                protected Database getDatabase()
                {
                    Database mockDb = Mockito.mock(Database.class);
                    try
                    {
                        Mockito.when(mockDb.getTaskIdsByReferenceId(Mockito.anyString())).thenThrow(new DbException("test"));
                    }
                    catch (Exception ex)
                    {
                        ex.printStackTrace();
                        Assert.fail(ex.getMessage());
                    }
                    return mockDb;
                }

                @Override
                public void asyncExecuteTask(String taskId, String revision, String status)
                {
                    // No action
                }
            };
            Task task = new Task();
            task.setId(currentTaskId);
            task.setReferenceId(referenceId);
            ex.triggerTasksByReferenceId(task);
            Assert.fail("Expected exception");
        }
        catch (Exception ex)
        {
            Assert.assertTrue(ex instanceof DbException);
        }
    }

    @Test
    public void testDecayCombinations()
    {
        // Decay factor greater that Long.MAX_VALUE but not Infinity
        Assert.assertEquals(Executor.MAXIMUM_DECAY_MILLIS, Executor.getDecayedWaitTime(1000));

        // Decay factor in Infinity
        Assert.assertEquals(Executor.MAXIMUM_DECAY_MILLIS, Executor.getDecayedWaitTime(1100));

        List<Long> actuals = new ArrayList<>();
        for (int i = 0; i <= 20; i++)
        {
            actuals.add(Executor.getDecayedWaitTime(i));
        }
        System.out.println(actuals);
        Assert.assertTrue(actuals.get(0).longValue() < 21000);
        Assert.assertTrue(actuals.get(1).longValue() < 21000);
        Assert.assertTrue(actuals.get(2).longValue() < 21000);

        // 1 second delay for the 0th adjusted retry count
        Assert.assertEquals(21000, actuals.get(3).longValue());

        // 18th retry onwards we peak
        Assert.assertEquals(Executor.MAXIMUM_DECAY_MILLIS, actuals.get(18).longValue());
        Assert.assertEquals(Executor.MAXIMUM_DECAY_MILLIS, actuals.get(19).longValue());
        Assert.assertEquals(Executor.MAXIMUM_DECAY_MILLIS, actuals.get(20).longValue());
    }

    @Test
    public void testCanExecuteFailedState()
    {
        final String taskId = "21312376120739821";
        final String revision = "3-213987312912";
        Task task = new Task();
        task.setId(taskId);
        task.setRevision(revision);
        task.setStatus(Status.NEW);
        task.setSequenceByReferenceId(false);
        task.setOwner("test");
        task.setFailedExecutionCount(0);
        Executor ex = new Executor("1")
        {
            @Override
            protected Database getDatabase()
            {
                try
                {
                    // get task

                    Database db = Mockito.mock(Database.class);
                    Mockito.when(db.getTask(Mockito.anyString(), Mockito.anyString())).thenReturn(task);

                    // get profile
                    Profile profile = new Profile();
                    Mockito.when(db.getProfile(Mockito.anyString())).thenReturn(profile);

                    // update task
                    Mockito.when(db.updateTask(Mockito.any())).thenReturn(task);

                    return db;
                }
                catch (DbException dbEx)
                {
                    dbEx.printStackTrace();
                    Assert.fail(dbEx.getMessage());
                    return null;
                }
            }

            @Override
            protected Recipe getRecipe(String taskType, Profile profile)
            {
                return new AbstractRecipe()
                {
                    @Override
                    public Result execute(ExecutionContext executionContext) throws TaskExecutionException
                    {
                        FatalTaskExecutionException tehx = new FatalTaskExecutionException("TestException");
                        tehx.setTargetStatus(TargetStatus.BLOCKED);
                        throw tehx;
                    }
                };
            }

            @Override
            protected LockManager getLockManager()
            {
                return new LockManager()
                {
                    @Override
                    public Lock getLock(String mutex)
                    {
                        return new Lock()
                        {

                            @Override
                            public boolean isAcquired()
                            {
                                return true;
                            }

                            @Override
                            public boolean unlock()
                            {
                                return true;
                            }

                            @Override
                            public String getMutex()
                            {
                                return mutex;
                            }
                        };
                    }
                };
            }

            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }
        };
        ex.executeTask(taskId, revision);
        Assert.assertEquals(Status.BLOCKED, task.getStatus());
    }

    @Test
    public void testBlockedStateOnFatality()
    {
        final String taskId = "21312376120739821";
        final String revision = "3-213987312912";
        Task task = new Task();
        task.setId(taskId);
        task.setRevision(revision);
        task.setStatus(Status.NEW);
        task.setSequenceByReferenceId(false);
        task.setOwner("test");
        task.setFailedExecutionCount(0);
        Executor ex = new Executor("1")
        {
            @Override
            protected Database getDatabase()
            {
                try
                {
                    // get task

                    Database db = Mockito.mock(Database.class);
                    Mockito.when(db.getTask(Mockito.anyString(), Mockito.anyString())).thenReturn(task);

                    // get profile
                    Profile profile = new Profile();
                    Mockito.when(db.getProfile(Mockito.anyString())).thenReturn(profile);

                    // update task
                    Mockito.when(db.updateTask(Mockito.any())).thenReturn(task);

                    return db;
                }
                catch (DbException dbEx)
                {
                    dbEx.printStackTrace();
                    Assert.fail(dbEx.getMessage());
                    return null;
                }
            }

            @Override
            protected Recipe getRecipe(String taskType, Profile profile)
            {
                return new Recipe()
                {
                    @Override
                    public Result execute(ExecutionContext executionContext) throws TaskExecutionException
                    {
                        return new Result("nextStep");
                    }
                };
            }

            @Override
            protected Task runTask(Profile profile, Recipe recipe, Task task, ExecutionContext executionContext, Database db)
                    throws Throwable
            {
                FatalTaskExecutionException teEx = new FatalTaskExecutionException("test");
                teEx.setTargetStatus(TargetStatus.BLOCKED);
                throw teEx;
            }

            @Override
            protected LockManager getLockManager()
            {
                return new LockManager()
                {
                    @Override
                    public Lock getLock(String mutex)
                    {
                        return new Lock()
                        {

                            @Override
                            public boolean isAcquired()
                            {
                                return true;
                            }

                            @Override
                            public boolean unlock()
                            {
                                return true;
                            }

                            @Override
                            public String getMutex()
                            {
                                return mutex;
                            }
                        };
                    }
                };
            }

            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }
        };
        ex.executeTask(taskId, revision);
        Assert.assertEquals(Status.BLOCKED, task.getStatus());
    }

    @Test
    public void testBlockedStateOnRetriesDone()
    {
        final String taskId = "21312376120739821";
        final String revision = "3-213987312912";
        Task task = new Task();
        task.setId(taskId);
        task.setRevision(revision);
        task.setStatus(Status.NEW);
        task.setSequenceByReferenceId(false);
        task.setOwner("test");
        Executor ex = new Executor("1")
        {
            @Override
            protected Database getDatabase()
            {
                try
                {
                    // get task

                    Database db = Mockito.mock(Database.class);
                    Mockito.when(db.getTask(Mockito.anyString(), Mockito.anyString())).thenReturn(task);

                    // get profile
                    Profile profile = Mockito.mock(Profile.class);
                    ExecutionConfig execConf = Mockito.mock(ExecutionConfig.class);
                    Mockito.when(execConf.getMaxRetries()).thenReturn(5);
                    Mockito.when(profile.getExecutionConfig()).thenReturn(execConf);
                    Mockito.when(profile.getConfiguration()).thenReturn(new JsonObject());

                    Mockito.when(db.getProfile(Mockito.anyString())).thenReturn(profile);

                    // update task
                    Mockito.when(db.updateTask(Mockito.any())).thenReturn(task);

                    return db;
                }
                catch (DbException dbEx)
                {
                    dbEx.printStackTrace();
                    Assert.fail(dbEx.getMessage());
                    return null;
                }
            }

            @Override
            protected Recipe getRecipe(String taskType, Profile profile)
            {
                return new Recipe()
                {
                    @Override
                    public Result execute(ExecutionContext executionContext) throws TaskExecutionException
                    {
                        return new Result("nextStep");
                    }
                };
            }

            @Override
            protected LockManager getLockManager()
            {
                return new LockManager()
                {
                    @Override
                    public Lock getLock(String mutex)
                    {
                        return new Lock()
                        {

                            @Override
                            public boolean isAcquired()
                            {
                                return true;
                            }

                            @Override
                            public boolean unlock()
                            {
                                return true;
                            }

                            @Override
                            public String getMutex()
                            {
                                return mutex;
                            }
                        };
                    }
                };
            }

            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

            @Override
            protected Task runTask(Profile profile, Recipe recipe, Task task, ExecutionContext executionContext, Database db)
                    throws Throwable
            {
                TaskExecutionException teEx = new TaskExecutionException("test");
                teEx.setTargetStatus(TargetStatus.BLOCKED);
                throw teEx;
            }
        };
        task.setFailedExecutionCount(3);
        ex.executeTask(taskId, revision);
        Assert.assertEquals(Status.FAILED, task.getStatus());

        task.setFailedExecutionCount(4);
        ex.executeTask(taskId, revision);
        Assert.assertEquals(Status.FAILED, task.getStatus());
    }

    @Test
    public void testReadyToExecute()
    {
        TaskResult resultByReferenceId = new TaskResult(new ArrayList<JsonObject>(), "", 0);
        TaskResult resultByParentReferenceId = new TaskResult(new ArrayList<JsonObject>(), "", 0);
        final String refId = "refId";
        final String parentRefId = "parentRefId";

        Task task = new Task();
        task.setId("task1");
        task.setCreatedDate(new Date());
        task.setReferenceId(refId);
        task.setParentReferenceId(parentRefId);
        Executor ex = new Executor("instanceId")
        {
        };
        try
        {
            Assert.assertTrue(ex.readyToExecute(task, resultByReferenceId, resultByParentReferenceId));
        }
        catch (DbException dbEx)
        {
            Assert.fail(dbEx.getMessage());
            dbEx.printStackTrace();
        }

        ex = new Executor("instanceId")
        {
        };
        try
        {
            Assert.assertTrue(ex.readyToExecute(task, resultByReferenceId, resultByParentReferenceId));
        }
        catch (DbException dbEx)
        {
            Assert.fail(dbEx.getMessage());
            dbEx.printStackTrace();
        }
    }

    @Test
    public void testDeferredTaskMovement()
    {
        List<JsonObject> taskJsonList = new ArrayList<>();
        JsonObject taskJson = new JsonObject();
        taskJson.addProperty("status", "BLOCKED");
        taskJsonList.add(taskJson);

        final String refId = "refId";
        final String parentRefId = "parentRefId";
        Task task = new Task();
        task.setId("task1");
        Calendar currDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        currDate.add(Calendar.DATE, -6);
        task.setCreatedDate(currDate.getTime());
        task.setSequenceByReferenceId(true);
        task.setSequenceByParentReferenceId(true);
        task.setReferenceId(refId);
        task.setParentReferenceId(parentRefId);
        Executor ex = new Executor("1")
        {
            @Override
            protected Database getDatabase()
            {
                try
                {
                    // get task

                    Database db = Mockito.mock(Database.class);
                    Mockito.when(db.getTask(Mockito.anyString(), Mockito.anyString())).thenReturn(task);

                    // get profile
                    Profile profile = Mockito.mock(Profile.class);
                    ExecutionConfig execConf = Mockito.mock(ExecutionConfig.class);
                    Mockito.when(execConf.getMaxRetries()).thenReturn(5);
                    Mockito.when(profile.getExecutionConfig()).thenReturn(execConf);
                    Mockito.when(profile.getConfiguration()).thenReturn(new JsonObject());

                    Mockito.when(db.getProfile(Mockito.anyString())).thenReturn(profile);

                    // update task
                    Mockito.when(db.updateTask(Mockito.any())).thenReturn(task);
                    TaskResult resultByReferenceId = new TaskResult(taskJsonList, "", 1);
                    Mockito.when(db.getResultByIndexQuery(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                            Mockito.anyBoolean(), Mockito.anyInt(), Mockito.anyObject())).thenReturn(resultByReferenceId);

                    return db;
                }
                catch (DbException dbEx)
                {
                    dbEx.printStackTrace();
                    Assert.fail(dbEx.getMessage());
                    return null;
                }
            }

            @Override
            protected Recipe getRecipe(String taskType, Profile profile)
            {
                return new Recipe()
                {
                    @Override
                    public Result execute(ExecutionContext executionContext) throws TaskExecutionException
                    {
                        return new Result("nextStep");
                    }
                };
            }

            @Override
            protected LockManager getLockManager()
            {
                return new LockManager()
                {
                    @Override
                    public Lock getLock(String mutex)
                    {
                        return new Lock()
                        {

                            @Override
                            public boolean isAcquired()
                            {
                                return true;
                            }

                            @Override
                            public boolean unlock()
                            {
                                return true;
                            }

                            @Override
                            public String getMutex()
                            {
                                return mutex;
                            }
                        };
                    }
                };
            }

            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

            @Override
            protected Task runTask(Profile profile, Recipe recipe, Task task, ExecutionContext executionContext, Database db)
                    throws Throwable
            {
                TaskExecutionException teEx = new TaskExecutionException("test");
                teEx.setTargetStatus(TargetStatus.BLOCKED);
                throw teEx;
            }
        };
        ex.executeTask("task1", "");
        Assert.assertTrue(task.getStatus().equals(Status.DEFERRED));
    }

    @Test
    public void testDeferredTaskMovementWithParentPolling()
    {
        List<JsonObject> taskJsonList = new ArrayList<>();
        JsonObject taskJson = new JsonObject();
        taskJson.addProperty("status", "POLLING");
        taskJsonList.add(taskJson);

        final String refId = "refId";
        final String parentRefId = "parentRefId";
        Task task = new Task();
        task.setId("task1");
        Calendar currDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        currDate.add(Calendar.DATE, -6);
        task.setCreatedDate(currDate.getTime());
        task.setSequenceByReferenceId(true);
        task.setSequenceByParentReferenceId(true);
        task.setReferenceId(refId);
        task.setParentReferenceId(parentRefId);
        Executor ex = new Executor("1")
        {
            @Override
            protected Database getDatabase()
            {
                try
                {
                    // get task

                    Database db = Mockito.mock(Database.class);
                    Mockito.when(db.getTask(Mockito.anyString(), Mockito.anyString())).thenReturn(task);

                    // get profile
                    Profile profile = Mockito.mock(Profile.class);
                    ExecutionConfig execConf = Mockito.mock(ExecutionConfig.class);
                    Mockito.when(execConf.getMaxRetries()).thenReturn(5);
                    Mockito.when(profile.getExecutionConfig()).thenReturn(execConf);
                    Mockito.when(profile.getConfiguration()).thenReturn(new JsonObject());

                    Mockito.when(db.getProfile(Mockito.anyString())).thenReturn(profile);

                    // update task
                    Mockito.when(db.updateTask(Mockito.any())).thenReturn(task);
                    TaskResult resultByReferenceId = new TaskResult(taskJsonList, "", 1);
                    Mockito.when(db.getResultByIndexQuery(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                            Mockito.anyBoolean(), Mockito.anyInt(), Mockito.anyObject())).thenReturn(resultByReferenceId);

                    return db;
                }
                catch (DbException dbEx)
                {
                    dbEx.printStackTrace();
                    Assert.fail(dbEx.getMessage());
                    return null;
                }
            }

            @Override
            protected Recipe getRecipe(String taskType, Profile profile)
            {
                return new Recipe()
                {
                    @Override
                    public Result execute(ExecutionContext executionContext) throws TaskExecutionException
                    {
                        return new Result("nextStep");
                    }
                };
            }

            @Override
            protected LockManager getLockManager()
            {
                return new LockManager()
                {
                    @Override
                    public Lock getLock(String mutex)
                    {
                        return new Lock()
                        {

                            @Override
                            public boolean isAcquired()
                            {
                                return true;
                            }

                            @Override
                            public boolean unlock()
                            {
                                return true;
                            }

                            @Override
                            public String getMutex()
                            {
                                return mutex;
                            }
                        };
                    }
                };
            }

            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

            @Override
            protected Task runTask(Profile profile, Recipe recipe, Task task, ExecutionContext executionContext, Database db)
                    throws Throwable
            {
                TaskExecutionException teEx = new TaskExecutionException("test");
                teEx.setTargetStatus(TargetStatus.BLOCKED);
                throw teEx;
            }
        };
        ex.executeTask("task1", "");
        Assert.assertTrue(task.getStatus().equals(Status.DEFERRED));
    }

    @Test
    public void testDeferredTaskMovementWithParentDeferred()
    {
        List<JsonObject> taskJsonList = new ArrayList<>();
        JsonObject taskJson = new JsonObject();
        taskJson.addProperty("status", "DEFERRED");
        taskJsonList.add(taskJson);

        final String refId = "refId";
        final String parentRefId = "parentRefId";
        Task task = new Task();
        task.setId("task1");
        Calendar currDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        currDate.add(Calendar.DATE, -6);
        task.setCreatedDate(currDate.getTime());
        task.setSequenceByReferenceId(true);
        task.setSequenceByParentReferenceId(true);
        task.setReferenceId(refId);
        task.setParentReferenceId(parentRefId);
        Executor ex = new Executor("1")
        {
            @Override
            protected Database getDatabase()
            {
                try
                {
                    // get task

                    Database db = Mockito.mock(Database.class);
                    Mockito.when(db.getTask(Mockito.anyString(), Mockito.anyString())).thenReturn(task);

                    // get profile
                    Profile profile = Mockito.mock(Profile.class);
                    ExecutionConfig execConf = Mockito.mock(ExecutionConfig.class);
                    Mockito.when(execConf.getMaxRetries()).thenReturn(5);
                    Mockito.when(profile.getExecutionConfig()).thenReturn(execConf);
                    Mockito.when(profile.getConfiguration()).thenReturn(new JsonObject());

                    Mockito.when(db.getProfile(Mockito.anyString())).thenReturn(profile);

                    // update task
                    Mockito.when(db.updateTask(Mockito.any())).thenReturn(task);
                    TaskResult resultByReferenceId = new TaskResult(taskJsonList, "", 1);
                    Mockito.when(db.getResultByIndexQuery(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                            Mockito.anyBoolean(), Mockito.anyInt(), Mockito.anyObject())).thenReturn(resultByReferenceId);

                    return db;
                }
                catch (DbException dbEx)
                {
                    dbEx.printStackTrace();
                    Assert.fail(dbEx.getMessage());
                    return null;
                }
            }

            @Override
            protected Recipe getRecipe(String taskType, Profile profile)
            {
                return new Recipe()
                {
                    @Override
                    public Result execute(ExecutionContext executionContext) throws TaskExecutionException
                    {
                        return new Result("nextStep");
                    }
                };
            }

            @Override
            protected LockManager getLockManager()
            {
                return new LockManager()
                {
                    @Override
                    public Lock getLock(String mutex)
                    {
                        return new Lock()
                        {

                            @Override
                            public boolean isAcquired()
                            {
                                return true;
                            }

                            @Override
                            public boolean unlock()
                            {
                                return true;
                            }

                            @Override
                            public String getMutex()
                            {
                                return mutex;
                            }
                        };
                    }
                };
            }

            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

            @Override
            protected Task runTask(Profile profile, Recipe recipe, Task task, ExecutionContext executionContext, Database db)
                    throws Throwable
            {
                TaskExecutionException teEx = new TaskExecutionException("test");
                teEx.setTargetStatus(TargetStatus.BLOCKED);
                throw teEx;
            }
        };
        ex.executeTask("task1", "");
        Assert.assertTrue(task.getStatus().equals(Status.DEFERRED));
    }

    @Test
    public void testDeferredTaskMovementWithParentNa()
    {
        List<JsonObject> taskJsonList = new ArrayList<>();
        JsonObject taskJson = new JsonObject();
        taskJson.addProperty("status", "NEEDS_ATTENTION");
        taskJsonList.add(taskJson);

        final String refId = "refId";
        final String parentRefId = "parentRefId";
        Task task = new Task();
        task.setId("task1");
        Calendar currDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        currDate.add(Calendar.DATE, -6);
        task.setCreatedDate(currDate.getTime());
        task.setSequenceByReferenceId(true);
        task.setSequenceByParentReferenceId(true);
        task.setReferenceId(refId);
        task.setParentReferenceId(parentRefId);
        Executor ex = new Executor("1")
        {
            @Override
            protected Database getDatabase()
            {
                try
                {
                    // get task

                    Database db = Mockito.mock(Database.class);
                    Mockito.when(db.getTask(Mockito.anyString(), Mockito.anyString())).thenReturn(task);

                    // get profile
                    Profile profile = Mockito.mock(Profile.class);
                    ExecutionConfig execConf = Mockito.mock(ExecutionConfig.class);
                    Mockito.when(execConf.getMaxRetries()).thenReturn(5);
                    Mockito.when(profile.getExecutionConfig()).thenReturn(execConf);
                    Mockito.when(profile.getConfiguration()).thenReturn(new JsonObject());

                    Mockito.when(db.getProfile(Mockito.anyString())).thenReturn(profile);

                    // update task
                    Mockito.when(db.updateTask(Mockito.any())).thenReturn(task);
                    TaskResult resultByReferenceId = new TaskResult(taskJsonList, "", 1);
                    Mockito.when(db.getResultByIndexQuery(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                            Mockito.anyBoolean(), Mockito.anyInt(), Mockito.anyObject())).thenReturn(resultByReferenceId);

                    return db;
                }
                catch (DbException dbEx)
                {
                    dbEx.printStackTrace();
                    Assert.fail(dbEx.getMessage());
                    return null;
                }
            }

            @Override
            protected Recipe getRecipe(String taskType, Profile profile)
            {
                return new Recipe()
                {
                    @Override
                    public Result execute(ExecutionContext executionContext) throws TaskExecutionException
                    {
                        return new Result("nextStep");
                    }
                };
            }

            @Override
            protected LockManager getLockManager()
            {
                return new LockManager()
                {
                    @Override
                    public Lock getLock(String mutex)
                    {
                        return new Lock()
                        {

                            @Override
                            public boolean isAcquired()
                            {
                                return true;
                            }

                            @Override
                            public boolean unlock()
                            {
                                return true;
                            }

                            @Override
                            public String getMutex()
                            {
                                return mutex;
                            }
                        };
                    }
                };
            }

            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

            @Override
            protected Task runTask(Profile profile, Recipe recipe, Task task, ExecutionContext executionContext, Database db)
                    throws Throwable
            {
                TaskExecutionException teEx = new TaskExecutionException("test");
                teEx.setTargetStatus(TargetStatus.BLOCKED);
                throw teEx;
            }
        };
        ex.executeTask("task1", "");
        Assert.assertTrue(task.getStatus().equals(Status.DEFERRED));
    }

    @Test
    public void testCompletedTaskWithGetTask()
    {
        Task task = new Task();
        task.setId("task1");
        Calendar currDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        currDate.add(Calendar.DATE, -6);
        task.setCreatedDate(currDate.getTime());
        task.setStatus(Status.COMPLETED);
        Executor ex = new Executor("1")
        {
            @Override
            protected Database getDatabase()
            {
                try
                {
                    // get task

                    Database db = Mockito.mock(Database.class);
                    Mockito.when(db.getTask(Mockito.anyString(), Mockito.anyString())).thenReturn(task);

                    return db;
                }
                catch (DbException dbEx)
                {
                    dbEx.printStackTrace();
                    Assert.fail(dbEx.getMessage());
                    return null;
                }
            }

            @Override
            protected LockManager getLockManager()
            {
                return new LockManager()
                {
                    @Override
                    public Lock getLock(String mutex)
                    {
                        return new Lock()
                        {

                            @Override
                            public boolean isAcquired()
                            {
                                return true;
                            }

                            @Override
                            public boolean unlock()
                            {
                                return true;
                            }

                            @Override
                            public String getMutex()
                            {
                                return mutex;
                            }
                        };
                    }
                };
            }

            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

        };
        ex.executeTask("task1", "");
        Assert.assertTrue(task.getStatus().equals(Status.COMPLETED));
    }

    @Test
    public void testRunningTaskWithGetTask()
    {
        Task task = new Task();
        task.setId("task1");
        Calendar currDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        currDate.add(Calendar.DATE, -6);
        task.setCreatedDate(currDate.getTime());
        task.setStatus(Status.RUNNING);
        task.setPreferredExecutionDate(DateUtilities.getDate(System.currentTimeMillis() + (10 * 60 * 1000)));
        Executor ex = new Executor("1")
        {
            @Override
            protected Database getDatabase()
            {
                try
                {
                    // get task

                    Database db = Mockito.mock(Database.class);
                    Mockito.when(db.getTask(Mockito.anyString(), Mockito.anyString())).thenReturn(task);

                    return db;
                }
                catch (DbException dbEx)
                {
                    dbEx.printStackTrace();
                    Assert.fail(dbEx.getMessage());
                    return null;
                }
            }

            @Override
            protected LockManager getLockManager()
            {
                return new LockManager()
                {
                    @Override
                    public Lock getLock(String mutex)
                    {
                        return new Lock()
                        {

                            @Override
                            public boolean isAcquired()
                            {
                                return true;
                            }

                            @Override
                            public boolean unlock()
                            {
                                return true;
                            }

                            @Override
                            public String getMutex()
                            {
                                return mutex;
                            }
                        };
                    }
                };
            }

            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

        };
        ex.executeTask("task1", "");
        Assert.assertTrue(task.getStatus().equals(Status.SCHEDULED));
    }

    @Test
    public void testGetTaskThrowException()
    {
        Task task = new Task();
        task.setId("task1");
        Calendar currDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        currDate.add(Calendar.DATE, -6);
        task.setCreatedDate(currDate.getTime());
        task.setStatus(Status.COMPLETED);
        Executor ex = new Executor("1")
        {
            @Override
            protected Database getDatabase()
            {
                try
                {
                    // get task

                    Database db = Mockito.mock(Database.class);
                    Mockito.when(db.getTask(Mockito.anyString(), Mockito.anyString())).thenThrow(new NoDocumentException(""));

                    return db;
                }
                catch (DbException dbEx)
                {
                    dbEx.printStackTrace();
                    Assert.fail(dbEx.getMessage());
                    return null;
                }
            }

            @Override
            protected LockManager getLockManager()
            {
                return new LockManager()
                {
                    @Override
                    public Lock getLock(String mutex)
                    {
                        return new Lock()
                        {

                            @Override
                            public boolean isAcquired()
                            {
                                return true;
                            }

                            @Override
                            public boolean unlock()
                            {
                                return true;
                            }

                            @Override
                            public String getMutex()
                            {
                                return mutex;
                            }
                        };
                    }
                };
            }

            @Override
            protected Task markTaskRunning(Task task, Database db) throws DbException
            {
                task.setStatus(Status.RUNNING);
                return task;
            }

        };
        ex.executeTask("task1", "");

    }
}
