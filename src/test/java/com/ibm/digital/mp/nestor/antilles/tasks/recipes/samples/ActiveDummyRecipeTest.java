package com.ibm.digital.mp.nestor.antilles.tasks.recipes.samples;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.support.membermodification.MemberModifier;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.tasks.ExecutionContext;
import com.ibm.digital.mp.nestor.tasks.ReservedTaskSteps;
import com.ibm.digital.mp.nestor.tasks.Result;
import com.ibm.digital.mp.nestor.tasks.Result.Status;
import com.ibm.digital.mp.nestor.tasks.TaskExecutionException;
import com.ibm.digital.mp.nestor.tasks.recipes.util.RecipeUtilities;
import com.ibm.digital.mp.nestor.tasks.recipes.util.Response;

public class ActiveDummyRecipeTest
{
    @Test
    public void testExecuteStart()
    {
        ActiveDummyRecipe recipe = new ActiveDummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(),
                new JsonObject(), new JsonObject(), ReservedTaskSteps.START_STEP, 0, 0);

        try
        {
            Result result = recipe.execute(execContext);
            Assert.assertEquals(Status.RUNNING, result.getStatus());
            Assert.assertEquals("SUBMIT_TASKS", result.getNextStep());

        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void testExecuteSubmitTasks()
    {
        ActiveDummyRecipe recipe = new ActiveDummyRecipe();
        JsonObject config = new JsonObject();
        config.addProperty("subject", "12345");
        config.addProperty("secret", "12345");
        config.addProperty("antillesBaseUrl", "http//antilles");

        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(),
                new JsonObject(), config, "SUBMIT_TASKS", 0, 0);
        RecipeUtilities recipeUtil = PowerMock.createMock(RecipeUtilities.class);
        Response response = PowerMock.createMock(Response.class);

        try
        {
            EasyMock.expect(recipeUtil.sendHttpRequest(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                    EasyMock.anyObject(), EasyMock.anyObject())).andReturn(response).anyTimes();
            MemberModifier.field(ActiveDummyRecipe.class, "recipeUtil").set(recipe, recipeUtil);
            EasyMock.expect(response.getEntity()).andReturn("{\"id\":1}").anyTimes();
            PowerMock.replayAll(recipeUtil, response);

            Result result = recipe.execute(execContext);
            Assert.assertEquals(Status.RUNNING, result.getStatus());
            Assert.assertEquals("CHECK_TASKS_FOR_COMPLETION", result.getNextStep());

        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void testExecuteCheckTaskForCompletion()
    {
        ActiveDummyRecipe recipe = new ActiveDummyRecipe();
        JsonObject config = new JsonObject();
        config.addProperty("subject", "12345");
        config.addProperty("secret", "12345");
        config.addProperty("antillesBaseUrl", "http//antilles");
        JsonObject vault = new JsonObject();
        vault.addProperty("referenceId", "1234");

        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(), vault,
                config, "CHECK_TASKS_FOR_COMPLETION", 0, 0);
        RecipeUtilities recipeUtil = PowerMock.createMock(RecipeUtilities.class);
        Response response = PowerMock.createMock(Response.class);

        try
        {

            JsonArray entityArray = new JsonArray();
            JsonObject activeTask = new JsonObject();
            activeTask.addProperty("status", "ACTIVE");
            entityArray.add(activeTask);
            JsonObject queuedTask = new JsonObject();
            queuedTask.addProperty("status", "QUEUED");
            entityArray.add(queuedTask);
            JsonObject failedTask = new JsonObject();
            failedTask.addProperty("status", "FAILED");
            entityArray.add(failedTask);
            JsonObject newTask = new JsonObject();
            newTask.addProperty("status", "NEW");
            entityArray.add(newTask);
            entityArray.add(newTask);
            JsonObject entity = new JsonObject();
            entity.add("data", entityArray);
            EasyMock.expect(recipeUtil.sendHttpRequest(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                    EasyMock.anyObject(), EasyMock.anyObject())).andReturn(response).anyTimes();
            MemberModifier.field(ActiveDummyRecipe.class, "recipeUtil").set(recipe, recipeUtil);
            EasyMock.expect(response.getEntity()).andReturn(entity.toString()).anyTimes();
            PowerMock.replayAll(recipeUtil, response);

            Result result = recipe.execute(execContext);
            Assert.assertEquals(Status.RUNNING, result.getStatus());
            Assert.assertEquals(ReservedTaskSteps.END_STEP, result.getNextStep());

        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void testExecuteEnd()
    {
        ActiveDummyRecipe recipe = new ActiveDummyRecipe();
        ExecutionContext execContext = new ExecutionContext(null, "158c25a01aec4b0a90efb18", "mytest10khklj", new JsonObject(),
                new JsonObject(), new JsonObject(), ReservedTaskSteps.END_STEP, 0, 0);

        try
        {
            Result result = recipe.execute(execContext);
            Assert.assertNull(result);

        }
        catch (TaskExecutionException ex)
        {
            Assert.fail(ex.getMessage());
        }
    }
}
