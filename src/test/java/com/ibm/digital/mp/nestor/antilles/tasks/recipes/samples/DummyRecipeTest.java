package com.ibm.digital.mp.nestor.antilles.tasks.recipes.samples;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.tasks.ExecutionContext;
import com.ibm.digital.mp.nestor.tasks.ReservedTaskSteps;
import com.ibm.digital.mp.nestor.tasks.Result;
import com.ibm.digital.mp.nestor.tasks.Result.Status;
import com.ibm.digital.mp.nestor.tasks.Task;
import com.ibm.digital.mp.nestor.tasks.TaskExecutionException;

public class DummyRecipeTest
{
    @Test
    public void testExecuteStart()
    {
        DummyRecipe recipe = new DummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(),
                new JsonObject(), new JsonObject(), ReservedTaskSteps.START_STEP, 0, 0);

        try
        {
            Result result = recipe.execute(execContext);
            Assert.assertEquals(Status.RUNNING, result.getStatus());
            Assert.assertEquals("EXECUTION_STEP", result.getNextStep());

        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }
    
    @Test
    public void testExecuteExecutionStep()
    {
        DummyRecipe recipe = new DummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(),
                new JsonObject(), new JsonObject(), "EXECUTION_STEP", 0, 0);

        try
        {
            Result result = recipe.execute(execContext);
            Assert.assertEquals(Status.RUNNING, result.getStatus());
            Assert.assertEquals(Task.END_STEP, result.getNextStep());

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
        DummyRecipe recipe = new DummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(),
                new JsonObject(), new JsonObject(), ReservedTaskSteps.END_STEP, 0, 0);

        try
        {
            Result result = recipe.execute(execContext);
            Assert.assertEquals(Status.COMPLETED, result.getStatus());

        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }
    
    @Test
    public void testExecuteNullStep()
    {
        DummyRecipe recipe = new DummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(),
                new JsonObject(), new JsonObject(), null, 0, 0);

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
        DummyRecipe recipe = new DummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(),
                null, new JsonObject(),"EXECUTION_STEP", 0, 0);

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
        DummyRecipe recipe = new DummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", null,
                new JsonObject(),new JsonObject() ,ReservedTaskSteps.END_STEP, 0, 0);

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
    
}
