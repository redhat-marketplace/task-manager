package com.ibm.digital.mp.nestor.internal.antilles.maintenance.recipes;

import java.util.Calendar;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.db.DbException;
import com.ibm.digital.mp.nestor.db.DbFactory;
import com.ibm.digital.mp.nestor.tasks.ExecutionContext;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.tasks.ReservedTaskSteps;
import com.ibm.digital.mp.nestor.tasks.Result;
import com.ibm.digital.mp.nestor.tasks.Result.Status;
import com.ibm.digital.mp.nestor.tasks.TaskExecutionException;
import com.ibm.digital.mp.nestor.tasks.recipes.AbstractRecipe;

public class TaskPurger extends AbstractRecipe
{

    private static final Logger logger = LogManager.getLogger(TaskPurger.class);

    private static final String FETCH_PURGE_STEP = "fetchAndPurge";
    private static final String PREPARE = "prepare";
    private static final String STEP_DATA_SEPARATOR = ":";
    private static final long TIME_DELAY_BETWEEN_FETCH_PURGE_STEP = 5000;

    @Override
    public Result execute(ExecutionContext executionContext) throws TaskExecutionException
    {
        String nextStep = executionContext.getStepId();
        logger.info("Executing step {} for taskId {}", nextStep, executionContext.getTaskId());
        try
        {
            // 1 get the next module name to be purge
            // 1.1 if next module Name is null then user generic flow (all records - already covered modules)
            // 2 FETCH 200 records to be purge from archive db
            // 3 BUILD payload for purge
            // 4 DELETE records
            // 5 REPEAT Step 2 with next set of 200 records, schedule based on nextBatchSleepTime
            // 6 if no more records to purge
            // 5.1 move to next module to purge, schedule based on nextModuleSleepTime
            // 6 if all module data purged, schedule for next date, schedule based on nextBatchSleepTime

            JsonObject vault = executionContext.getVault();
            JsonObject config = executionContext.getConfiguration();

            switch (nextStep)
            {
                case ReservedTaskSteps.START_STEP:

                    return new Result(PREPARE, Status.RUNNING, new JsonObject());

                case PREPARE:

                    // build vault
                    return this.prepareVault(config, vault);

                default:

                    String[] stepData = nextStep.split(STEP_DATA_SEPARATOR);

                    if (FETCH_PURGE_STEP.equals(stepData[0]))
                    {

                        int recordsPerRuns = config.get("purgeRecordsPerRuns").getAsInt();
                        JsonArray purgedModules = vault.get("purgedModules").getAsJsonArray();

                        // fetch data to be purge
                        List<JsonObject> dataToPurge = DbFactory.getTaskArchiverDao().fetchTasksToPurge(
                                this.buildQuery(Integer.parseInt(stepData[2]), this.getOwnerQuery(stepData[1])), recordsPerRuns);

                        if (dataToPurge.isEmpty())
                        {
                            int currentStepIdx = vault.get("stepIndex").getAsInt();
                            if (currentStepIdx == purgedModules.size() - 1)
                            {
                                // no more data to purge, scheduled for next run
                                return new Result(ReservedTaskSteps.START_STEP, Status.RUNNING, new JsonObject(),
                                        this.getNextScheduleDate());
                            }
                            else
                            {
                                // go for next step
                                int stepIndex = currentStepIdx + 1;
                                vault.addProperty("stepIndex", stepIndex);
                                return new Result(vault.get("steps").getAsJsonArray().get(stepIndex).getAsString(), Status.RUNNING, vault);
                            }
                        }
                        else
                        {
                            // purge data
                            DbFactory.getTaskArchiverDao().purgeTask(dataToPurge);

                            long timeDelay = TIME_DELAY_BETWEEN_FETCH_PURGE_STEP;
                            if (config.get("defaultTimeDelayBetweenFetchAndPurge") != null)
                            {
                                timeDelay = config.get("defaultTimeDelayBetweenFetchAndPurge").getAsLong();
                            }

                            // execute same step again with next set of data to be archive, with 5 second gap.
                            return new Result(nextStep, Status.RUNNING, vault, (Calendar.getInstance().getTimeInMillis() + timeDelay));
                        }
                    }
                    else
                    {
                        throw new TaskExecutionException("wrong step");

                    }
            }

        }
        catch (Exception ex)
        {
            throw new TaskExecutionException(ex);
        }
    }

    private Result prepareVault(JsonObject config, JsonObject vault) throws DbException
    {
        JsonArray steps = new JsonArray();
        JsonElement purgeInDays;
        JsonArray purgedModules = new JsonArray();

        // fetch all profiles
        List<Profile> profiles = DbFactory.getDatabase().fetchAllProfiles();

        // defaultPurgePolicyInDays
        int defaultPurgeInDays = config.get("defaultPurgePolicyInDays").getAsInt();

        for (Profile profile : profiles)
        {
            purgeInDays = profile.getConfiguration().get("purgePolicyInDays");
            if (purgeInDays != null)
            {
                steps.add(FETCH_PURGE_STEP + STEP_DATA_SEPARATOR + profile.getId() + STEP_DATA_SEPARATOR + purgeInDays.getAsString());

                purgedModules.add(profile.getId());
            }
            else
            {
                steps.add(FETCH_PURGE_STEP + STEP_DATA_SEPARATOR + profile.getId() + STEP_DATA_SEPARATOR + defaultPurgeInDays);
                purgedModules.add(profile.getId());
            }
        }

        vault.add("steps", steps);
        vault.add("purgedModules", purgedModules);

        vault.addProperty("stepIndex", 0);
        // start with 1st step
        return new Result(steps.getAsJsonArray().get(0).getAsString(), Status.RUNNING, vault);
    }

    private String buildQuery(int purgeDayRange, String owner)
    {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, -purgeDayRange);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.AM_PM, Calendar.AM);

        StringBuilder query = new StringBuilder("status:\"COMPLETED\" AND modifiedDate: [0 TO ");
        query.append(cal.getTimeInMillis());
        query.append("]");
        if (StringUtils.isNotBlank(owner))
        {
            query.append(" AND ");
            query.append(owner);
        }

        return query.toString();
    }

    private long getNextScheduleDate()
    {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, +1);
        cal.set(Calendar.HOUR, 1);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.AM_PM, Calendar.AM);

        return cal.getTimeInMillis();
    }

    private String getOwnerQuery(String ownerName)
    {
        StringBuilder ownerQuery = new StringBuilder("owner: ");

        ownerQuery.append("\"").append(ownerName).append("\"");

        return ownerQuery.toString();
    }
}
