package com.ibm.digital.mp.nestor.antilles.tasks.recipes.samples;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.SystemException;
import com.ibm.digital.mp.nestor.tasks.ExecutionContext;
import com.ibm.digital.mp.nestor.tasks.ReservedTaskSteps;
import com.ibm.digital.mp.nestor.tasks.Result;
import com.ibm.digital.mp.nestor.tasks.Result.Status;
import com.ibm.digital.mp.nestor.tasks.TaskExecutionException;
import com.ibm.digital.mp.nestor.tasks.recipes.util.RecipeUtilities;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ RecipeUtilities.class })
public class FailingDummyRecipeTest
{
    @Test
    public void testExecuteStart()
    {
        FailingDummyRecipe recipe = new FailingDummyRecipe();
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
    public void testExecuteStartSystemException() throws Exception
    {
        RecipeUtilities util = PowerMock.createNiceMock(RecipeUtilities.class);
        PowerMock.expectNew(RecipeUtilities.class).andReturn(util).anyTimes();
        EasyMock.expect(util.sendHttpRequest("GET", "https://www.google.co.in/?gfe_rd=cr&dcr=0&ei=AuUXWqK0N-WK8Qe56KSQBg", null,
                null, "google")).andThrow(new SystemException("", new Throwable())).anyTimes();
        
        PowerMock.replayAll(util,RecipeUtilities.class);
        FailingDummyRecipe recipe = new FailingDummyRecipe();
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
        FailingDummyRecipe recipe = new FailingDummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(),
                new JsonObject(), new JsonObject(), "EXECUTION_STEP", 0, 0);

        try
        {
            recipe.execute(execContext);

        }
        catch (TaskExecutionException ex)
        {
            Assert.assertEquals("Recipe failing continuously", ex.getMessage());
        }
    }

    @Test
    public void testExecuteEnd()
    {
        FailingDummyRecipe recipe = new FailingDummyRecipe();
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
        FailingDummyRecipe recipe = new FailingDummyRecipe();
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
        FailingDummyRecipe recipe = new FailingDummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(), null,
                new JsonObject(), "EXECUTION_STEP", 0, 0);

        try
        {
            recipe.execute(execContext);
            Assert.fail("Expected a TaskExecutionException");
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertEquals("Recipe failing continuously", ex.getMessage());
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            Assert.fail("Expected a TaskExecutionException wrapping the cause.");
        }
    }

}
