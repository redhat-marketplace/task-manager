package com.ibm.digital.mp.nestor.antilles.tasks.recipes.samples;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.SystemException;
import com.ibm.digital.mp.nestor.tasks.ExecutionContext;
import com.ibm.digital.mp.nestor.tasks.Result;
import com.ibm.digital.mp.nestor.tasks.Task;
import com.ibm.digital.mp.nestor.tasks.TaskExecutionException;
import com.ibm.digital.mp.nestor.tasks.recipes.AbstractRecipe;
import com.ibm.digital.mp.nestor.tasks.recipes.util.RecipeUtilities;

public class FailingDummyRecipe extends AbstractRecipe
{
    private static final String SLEEP_TIME = "sleepTime";

    private static final Logger logger = LogManager.getLogger(DummyRecipe.class);

    @Override
    public Result execute(ExecutionContext executionContext) throws TaskExecutionException
    {
        Result result = null;
        String stepId = executionContext.getStepId();
        JsonObject payload = executionContext.getPayload();
        JsonObject vault = executionContext.getVault();
        if (Task.START_STEP.equals(stepId))
        {
            logger.info("Starting Dummy Task: " + payload.get("DUMMY"));
            try
            {
                new RecipeUtilities().sendHttpRequest("GET", "https://www.google.co.in/?gfe_rd=cr&dcr=0&ei=AuUXWqK0N-WK8Qe56KSQBg", null,
                        null, "google");
            }
            catch (SystemException ex)
            {
                // TODO Auto-generated catch block
                ex.printStackTrace();
            }
            return new Result("EXECUTION_STEP", Result.Status.RUNNING);
        }
        else if ("EXECUTION_STEP".equals(stepId))
        {
            return executionStep(payload, vault);

        }
        else if (Task.END_STEP.equals(stepId))
        {
            logger.info("Completing Dummy Task: " + payload.get("DUMMY"));
            return new Result(Task.END_STEP, Result.Status.COMPLETED);
        }

        return result;
    }

    private Result executionStep(JsonObject payload, JsonObject vault) throws TaskExecutionException
    {
        try
        {
            // Simulating work
            JsonObject parameters = new JsonObject();
            double multiplier = (Math.random() / 2) + 1.0;
            long sleepTime = (long) (2000 * multiplier);
            parameters.addProperty(SLEEP_TIME, sleepTime);
            vault.addProperty(SLEEP_TIME, sleepTime);
            payload.addProperty("DUMMY", sleepTime);

            if (parameters.has(SLEEP_TIME))
            {
                sleepTime = parameters.get(SLEEP_TIME).getAsLong();
            }
            Thread.sleep(sleepTime);
        }
        catch (Exception exception)
        {
            logger.error("Error while getting sleep time.", exception);
        }
        throw new TaskExecutionException("Recipe failing continuously");
    }

}
