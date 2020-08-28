package com.ibm.digital.mp.nestor.antilles.tasks.recipes.samples;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.tasks.ExecutionContext;
import com.ibm.digital.mp.nestor.tasks.Result;
import com.ibm.digital.mp.nestor.tasks.TaskExecutionException;
import com.ibm.digital.mp.nestor.tasks.recipes.AbstractRecipe;
import com.ibm.digital.mp.nestor.tasks.recipes.util.DateUtilities;

public class ScheduledDummyRecipe extends AbstractRecipe
{
    private static final String SLEEP_TIME = "sleepTime";

    private static final Logger logger = LogManager.getLogger(ScheduledDummyRecipe.class);

    @Override
    public Result execute(ExecutionContext executionContext) throws TaskExecutionException
    {
        String stepNumber = executionContext.getStepId();
        JsonObject payload = executionContext.getPayload();
        JsonObject vault = executionContext.getVault();
        long sleepTime;
        if (stepNumber.equals("START"))
        {
            try
            {
                logger.info("Starting Dummy Task: " + payload.get("id"));
                sleepTime = getSleepTime(vault, payload);
                Thread.sleep(sleepTime);
                logger.error("Completing Dummy Task: " + payload.get("DUMMY"));
            }
            catch (Throwable throwableException)
            {
                logger.error(throwableException.getMessage(), throwableException);
            }
            long preferredTime = System.currentTimeMillis() + 100000;
            return new Result("END", Result.Status.RUNNING, vault, DateUtilities.getDate(preferredTime).getTime());
        }
        else if (stepNumber.equals("END"))
        {
            return new Result("END", Result.Status.COMPLETED, vault, null);
        }
        return null;
    }

    protected long getSleepTime(JsonObject vault, JsonObject payload)
    {
        double multiplier = (Math.random() / 2) + 1.0;
        long sleepTime = (long) (2000 * multiplier);
        try
        {
            // Simulating work

            JsonObject parameters = new JsonObject();
            parameters.addProperty(SLEEP_TIME, sleepTime);
            vault.addProperty(SLEEP_TIME, sleepTime);
            payload.addProperty("DUMMY", sleepTime);

            if (parameters.has(SLEEP_TIME))
            {
                sleepTime = parameters.get(SLEEP_TIME).getAsLong();
            }
        }
        catch (Exception exception)
        {
            logger.error("Error while getting sleep time. {}", exception);
        }
        return sleepTime;
    }

}
