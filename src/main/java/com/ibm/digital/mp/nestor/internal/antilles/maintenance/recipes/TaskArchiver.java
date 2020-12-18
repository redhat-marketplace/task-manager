/********************************************************** {COPYRIGHT-TOP} ****
 * IBM Internal Use Only
 * IBM Marketplace SaaS Resource Manager
 *
 * (C) Copyright IBM Corp. 2020  All Rights Reserved.
 *
 * The source code for this program is not published or otherwise  
 * divested of its trade secrets, irrespective of what has been 
 * deposited with the U.S. Copyright Office.
 ********************************************************** {COPYRIGHT-END} ***/

package com.ibm.digital.mp.nestor.internal.antilles.maintenance.recipes;

import java.util.Calendar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.db.DbFactory;
import com.ibm.digital.mp.nestor.tasks.ExecutionContext;
import com.ibm.digital.mp.nestor.tasks.ReservedTaskSteps;
import com.ibm.digital.mp.nestor.tasks.Result;
import com.ibm.digital.mp.nestor.tasks.Result.Status;
import com.ibm.digital.mp.nestor.tasks.TaskExecutionException;
import com.ibm.digital.mp.nestor.tasks.recipes.AbstractRecipe;

public class TaskArchiver extends AbstractRecipe
{

    private static final Logger logger = LogManager.getLogger(TaskArchiver.class);

    private static final String PREPARE = "prepare";
    private static final String FIND_AND_COPY_STEP = "findAndCopy";
    private static final String FIND_AND_DELETE_STEP = "findAndDelete";
    private static final String VALIDATE_DATA = "validateData";
    private static final String REMOVE_DATA_STEP = "removeData";
    private static final String STEP_DATA_SEPARATOR = ":";
    private static final String ON_CANDID_ARCHIVE = "onCandid";
    private static final String FALLBACK_STEP_INDEX = "fallbackStepIndex";
    private static final String FETCH_MORE_RECORDS = "fetchMore";
    private static final String MIN_MODIFIED_DATE = "minModifiedDate";
    private static final String MAX_MODIFIED_DATE = "maxModifiedDate";
    private static final String ARCHIVED_TASK_IDS = "archivedTaskIds";
    private static final String STEPS = "steps";
    private static final String STEP_INDEX = "stepIndex";
    private static final String PURGED_MODULES = "purgedModules";
    private static final String DEFAULT_TIME_DELAY_COPY_AND_FIND = "defaultTimeDelayBetweenCopyAndFind";
    private static final String FETCH_BATCH_SIZE = "archiveFetchRecordsPerRuns";
    private static final String INSERT_BATCH_SIZE = "archiveInsertRecordsPerRuns";
    private static final String DISABLE_BULK_UPSERT = "disableBulkUpsert";

    private static final long TIME_DELAY_BETWEEN_FIND_COPY_STEP = 5000;
    private static final long MAX_ALLOWED_FETCH_BATCH_SIZE = 5000;

    @Override
    public Result execute(ExecutionContext executionContext) throws TaskExecutionException
    {
        String nextStep = executionContext.getStepId();
        logger.info("Executing step {} for taskId {}", nextStep, executionContext.getTaskId());
        try
        {
            // 1 get the next module name to be archive
            // 1.1 if next module Name is null then user generic flow (all records - already covered modules)
            // 2 FETCH 200 records to be archive from source db
            // 3 INSERT data to target db.
            // 4 VALIDATE data, count same in source and target db
            // 5 DELETE data for the module from source db.
            // 6 if more records for the same module need to archive,
            // REPEAT Step 2 with next set of 200 records, schedule based on nextBatchSleepTime
            // 7 move to next module to archive, schedule based on nextModuleSleepTime
            // 8 if all module data archived, schedule for next date, schedule based on nextBatchSleepTime

            JsonObject vault = executionContext.getVault();
            JsonObject config = executionContext.getConfiguration();

            switch (nextStep)
            {
                case ReservedTaskSteps.START_STEP:

                    return new Result(PREPARE, Status.RUNNING, new JsonObject());

                case PREPARE:

                    // build vault
                    TaskArchiverUtil.prepareVault(config, vault);

                    // start with 1st step
                    return new Result(vault.get(STEPS).getAsJsonArray().get(0).getAsString(), Status.RUNNING, vault);

                case FIND_AND_DELETE_STEP:

                    // find and Delete markForDeletion tasks
                    long deleteCount = DbFactory.getTaskArchiverDao().deleteTasks(this.buildMarkForDeleteQuery());
                    logger.info("deleted {} tasks, which were marked for deletion.", deleteCount);

                    // scheduled for next run
                    return new Result(ReservedTaskSteps.START_STEP, Status.RUNNING, new JsonObject(),
                            TaskArchiverUtil.getNextScheduleDate());

                default:
                    int stepIndex = vault.get(STEP_INDEX).getAsInt();

                    String[] stepsData = nextStep.split(STEP_DATA_SEPARATOR);

                    switch (stepsData[0])
                    {
                        case FIND_AND_COPY_STEP:

                            int fetchRecordsPerRuns = config.get(FETCH_BATCH_SIZE).getAsInt();
                            int insertRecordsPerRuns = config.get(INSERT_BATCH_SIZE).getAsInt();
                            boolean disabledBulkUpsert = false;
                            if (config.has(DISABLE_BULK_UPSERT))
                            {
                                disabledBulkUpsert = config.get(DISABLE_BULK_UPSERT).getAsBoolean();
                            }

                            // basic validation, archiveInsertRecordsPerRuns should be less than MAX_ALLOWED_FETCH_BATCH_SIZE
                            if (fetchRecordsPerRuns > MAX_ALLOWED_FETCH_BATCH_SIZE)
                            {
                                throw new TaskExecutionException("archiveInsertRecordsPerRuns is beyond allowed size ("
                                        + MAX_ALLOWED_FETCH_BATCH_SIZE + "), please update sytem profile.");
                            }

                            boolean onCandidStep = ON_CANDID_ARCHIVE.equals(stepsData[1]);
                            // fetch data from source db and persist in target db
                            JsonObject info = TaskArchiverUtil.findAndCopyData(Integer.parseInt(stepsData[2]),
                                    TaskArchiverUtil.getOwnerObject(stepsData[1], vault.get(PURGED_MODULES).getAsJsonArray()),
                                    fetchRecordsPerRuns, insertRecordsPerRuns, disabledBulkUpsert, onCandidStep);

                            boolean fetchMore = info.get(FETCH_MORE_RECORDS).getAsBoolean();
                            vault.addProperty(FETCH_MORE_RECORDS, fetchMore);
                            if (info.get(MIN_MODIFIED_DATE) != null)
                            {
                                vault.add(MIN_MODIFIED_DATE, info.get(MIN_MODIFIED_DATE));
                                vault.add(MAX_MODIFIED_DATE, info.get(MAX_MODIFIED_DATE));
                                vault.add(ARCHIVED_TASK_IDS, info.get(ARCHIVED_TASK_IDS));

                            }

                            if (fetchMore)
                            {
                                vault.addProperty(FALLBACK_STEP_INDEX, stepIndex);
                            }

                            // archiver done for current module, proceed to next step - validate data step
                            return this.nextStepResult(stepIndex, vault);

                        case VALIDATE_DATA:

                            // in-case no records archived, just move to next step
                            if (!vault.has(MIN_MODIFIED_DATE) || !vault.has(MAX_MODIFIED_DATE))
                            {
                                return this.nextStepResult(stepIndex, vault);
                            }

                            // validate all data copied successfully
                            // call source database to count records based on the archive date and archive date +1
                            long minModifiedDate = vault.get(MIN_MODIFIED_DATE).getAsLong();
                            long maxModifiedDate = vault.get(MAX_MODIFIED_DATE).getAsLong();

                            String targetQuery;
                            onCandidStep = ON_CANDID_ARCHIVE.equals(stepsData[1]);
                            if (onCandidStep)
                            {
                                targetQuery = TaskArchiverUtil.buildCountOfRecordsArchivedQuery(null, null, minModifiedDate,
                                        maxModifiedDate);
                            }
                            else
                            {
                                targetQuery = TaskArchiverUtil.buildCountOfRecordsArchivedQuery(stepsData[1],
                                        vault.get(PURGED_MODULES).getAsJsonArray(), minModifiedDate, maxModifiedDate);
                            }

                            long srcCount = vault.get(ARCHIVED_TASK_IDS).getAsJsonArray().size();
                            // call target database to count records based on the archive date and archive date +1

                            long targetCount = DbFactory.getTaskArchiverDao().countRecordsCopied(targetQuery);

                            // if both target and source count match, proceed to REMOVE_DATA_STEP step
                            // as still that date, target can have old existing data
                            if (srcCount <= targetCount)
                            {
                                return this.nextStepResult(stepIndex, vault);
                            }
                            else
                            {
                                // else, throw exception so that task move to NEED_ATTENTION state
                                throw new TaskExecutionException(
                                        "data valaidation failed, source count : " + srcCount + " target count " + targetCount);
                            }

                        case REMOVE_DATA_STEP:

                            // remove only if data archived
                            if (vault.has(ARCHIVED_TASK_IDS))
                            {

                                boolean removeStatus = DbFactory.getTaskArchiverDao()
                                        .removeArchivedTasks(vault.get(ARCHIVED_TASK_IDS).getAsJsonArray());

                                // in-case any exception while removing mongo data.
                                if (!removeStatus)
                                {
                                    throw new TaskExecutionException("Exeption in {} step.", nextStep);
                                }
                                fetchMore = vault.get(FETCH_MORE_RECORDS).getAsBoolean();
                                // in-case need to delete more records for the module
                                if (fetchMore)
                                {
                                    stepIndex = vault.get(FALLBACK_STEP_INDEX).getAsInt();
                                    vault.addProperty(STEP_INDEX, stepIndex);
                                    long timeDelay = TIME_DELAY_BETWEEN_FIND_COPY_STEP;
                                    if (config.get(DEFAULT_TIME_DELAY_COPY_AND_FIND) != null)
                                    {
                                        timeDelay = config.get(DEFAULT_TIME_DELAY_COPY_AND_FIND).getAsLong();
                                    }
                                    // execute findAndCopy step again for next set of data to be archive, with 5 second gap.
                                    return new Result(vault.get(STEPS).getAsJsonArray().get(stepIndex).getAsString(), Status.RUNNING, vault,
                                            (Calendar.getInstance().getTimeInMillis() + timeDelay));
                                }
                            }

                            // clear date range data before next module's findAndCopy step
                            vault.remove(MIN_MODIFIED_DATE);
                            vault.remove(MAX_MODIFIED_DATE);
                            vault.remove(ARCHIVED_TASK_IDS);
                            return this.nextStepResult(stepIndex, vault);

                        default:
                            return null;
                    }
            }

        }
        catch (Exception ex)
        {
            throw new TaskExecutionException(ex);
        }
    }

    /**
     * build mongo query for mark for deletion.
     * 
     * @return json query object.
     */
    private JsonObject buildMarkForDeleteQuery()
    {
        JsonObject query = new JsonObject();
        query.addProperty("markForDeletion", true);
        return query;
    }

    private Result nextStepResult(int stepIndex, JsonObject vault)
    {
        stepIndex++;
        vault.addProperty(STEP_INDEX, stepIndex);
        return new Result(vault.get(STEPS).getAsJsonArray().get(stepIndex).getAsString(), Status.RUNNING, vault);
    }
}
