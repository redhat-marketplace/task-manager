package com.ibm.digital.mp.nestor.antilles.tasks.recipes.samples;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.tasks.ExecutionContext;
import com.ibm.digital.mp.nestor.tasks.ReservedTaskSteps;
import com.ibm.digital.mp.nestor.tasks.Result;
import com.ibm.digital.mp.nestor.tasks.Result.Status;
import com.ibm.digital.mp.nestor.tasks.TaskExecutionException;

public class ScheduledDummyRecipeTest
{
    @Test
    public void testExecuteStart()
    {
        ScheduledDummyRecipe recipe = new ScheduledDummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(),
                new JsonObject(), new JsonObject(), ReservedTaskSteps.START_STEP, 0, 0);

        try
        {
            Result result = recipe.execute(execContext);
            Assert.assertEquals(Status.RUNNING, result.getStatus());
            Assert.assertEquals(ReservedTaskSteps.END_STEP, result.getNextStep());

        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }
    
    @Test
    public void testExecuteEnd()
    {
        ScheduledDummyRecipe recipe = new ScheduledDummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(),
                new JsonObject(), new JsonObject(), ReservedTaskSteps.END_STEP, 0, 0);

        try
        {
            Result result = recipe.execute(execContext);
            Assert.assertEquals(Status.COMPLETED, result.getStatus());
            Assert.assertEquals(ReservedTaskSteps.END_STEP, result.getNextStep());

        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }
    
    @Test
    public void testExecuteUnknowStep()
    {
        ScheduledDummyRecipe recipe = new ScheduledDummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(),
                new JsonObject(), new JsonObject(), "", 0, 0);

        try
        {
            Result result = recipe.execute(execContext);
            Assert.assertNull(result);

        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }
    
    @Test
    public void testExecuteNullVault()
    {
        ScheduledDummyRecipe recipe = new ScheduledDummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(),
                null, new JsonObject(),ReservedTaskSteps.START_STEP, 0, 0);

        try
        {
            Result result = recipe.execute(execContext);
            Assert.assertEquals(Status.RUNNING, result.getStatus());

        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }
    
    @Test
    public void testExecuteNullPayload()
    {
        ScheduledDummyRecipe recipe = new ScheduledDummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", null,
                new JsonObject(),new JsonObject() ,ReservedTaskSteps.START_STEP, 0, 0);

        try
        {
            Result result = recipe.execute(execContext);
            Assert.assertEquals(Status.RUNNING, result.getStatus());
            Assert.assertEquals(ReservedTaskSteps.END_STEP, result.getNextStep());

        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }
    
}
