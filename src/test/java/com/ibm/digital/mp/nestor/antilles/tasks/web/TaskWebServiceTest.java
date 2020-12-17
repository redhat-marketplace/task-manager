package com.ibm.digital.mp.nestor.antilles.tasks.web;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.logging.log4j.ThreadContext;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.antilles.tasks.Executor;
import com.ibm.digital.mp.nestor.client.AntillesClient;
import com.ibm.digital.mp.nestor.config.EnvironmentUtilities;
import com.ibm.digital.mp.nestor.db.Database;
import com.ibm.digital.mp.nestor.db.DbException;
import com.ibm.digital.mp.nestor.db.DbFactory;
import com.ibm.digital.mp.nestor.db.TaskResult;
import com.ibm.digital.mp.nestor.internal.antilles.tasks.recipes.RecipeManager;
import com.ibm.digital.mp.nestor.tasks.AllTasks;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.tasks.Status;
import com.ibm.digital.mp.nestor.tasks.Task;
import com.ibm.digital.mp.nestor.tasks.recipes.Recipe;
import com.ibm.digital.mp.nestor.util.Constants;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ AntillesClient.class, Executor.class, DbFactory.class, Database.class, RecipeManager.class, EnvironmentUtilities.class,
        ThreadContext.class })
@PowerMockIgnore({ "javax.management.*", "javax.crypto.*" })
public class TaskWebServiceTest
{

    @Test
    public void testCheck() throws DbException
    {
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Profile profile = new Profile();
        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService taskService = new TaskWebService();
        Response response = taskService.check();
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testCheckException() throws DbException
    {
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andThrow(new DbException("error"));
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService taskService = new TaskWebService();
        Response response = taskService.check();
        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public void testCreateTask() throws DbException
    {
        PowerMock.mockStatic(ThreadContext.class);
        EasyMock.expect(ThreadContext.getImmutableContext()).andReturn(new HashMap<String, String>()).anyTimes();
        EasyMock.expect(ThreadContext.getDepth()).andReturn(1).anyTimes();
        EasyMock.expect(ThreadContext.cloneStack()).andReturn(EasyMock.anyObject()).anyTimes();
        EasyMock.expect(ThreadContext.get(Constants.TRANSACTION_LOG_ID)).andReturn("123-234-345").anyTimes();
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        EasyMock.expect(request.getRequestURI()).andReturn("http://abc.com");
        EasyMock.expect(request.getHeader(Constants.TRANSACTION_ID_HEADER)).andReturn("71d29b88-9ee2-4e59-9214-fef72b8b07da").anyTimes();

        PowerMock.mockStatic(RecipeManager.class);
        Recipe recipe = PowerMock.createMock(Recipe.class);
        RecipeManager recipeManager = PowerMock.createMock(RecipeManager.class);
        EasyMock.expect(RecipeManager.getInstance()).andReturn(recipeManager).anyTimes();
        EasyMock.expect(recipeManager.getRecipe("RemoveSubscriptions", profile)).andReturn(recipe).anyTimes();

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setId("028582");
        EasyMock.expect(database.createTask(EasyMock.anyObject())).andReturn(task).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, ThreadContext.class, database);
        TaskWebService taskService = new TaskWebService();
        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\",\"preferredExecutionDate\":1516088453778,"
                + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,"
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";
        Response response = taskService.createTask(request, content, false);
        Assert.assertEquals(201, response.getStatus());
    }

    @Test
    public void testCreateTaskNoExecute() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        EasyMock.expect(request.getRequestURI()).andReturn("http://abc.com");
        EasyMock.expect(request.getHeader(Constants.TRANSACTION_ID_HEADER)).andReturn("71d29b88-9ee2-4e59-9214-fef72b8b07da").anyTimes();

        PowerMock.mockStatic(RecipeManager.class);
        Recipe recipe = PowerMock.createMock(Recipe.class);
        RecipeManager recipeManager = PowerMock.createMock(RecipeManager.class);
        EasyMock.expect(RecipeManager.getInstance()).andReturn(recipeManager).anyTimes();
        EasyMock.expect(recipeManager.getRecipe("RemoveSubscriptions", profile)).andReturn(recipe).anyTimes();

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        EasyMock.expect(database.createTask(EasyMock.anyObject())).andReturn(task).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService taskService = new TaskWebService()
        {
            @Override
            public Response executeTask(@Context HttpServletRequest request, @PathParam("id") String id,
                    @QueryParam("revision") String revision, @QueryParam("taskStatus") String taskStatus)
            {
                // Execute Task should not be called
                Assert.fail();
                return null;
            }
        };
        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\",\"preferredExecutionDate\":1516088453778,"
                + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,"
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";
        Response response = taskService.createTask(request, content, true);
        Assert.assertEquals(201, response.getStatus());
    }

    @Test
    public void testCreateTaskValidationFail() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        EasyMock.expect(request.getRequestURI()).andReturn("http://abc.com");
        EasyMock.expect(request.getHeader(Constants.TRANSACTION_ID_HEADER)).andReturn("71d29b88-9ee2-4e59-9214-fef72b8b07da").anyTimes();

        PowerMock.mockStatic(RecipeManager.class);
        Recipe recipe = PowerMock.createMock(Recipe.class);
        RecipeManager recipeManager = PowerMock.createMock(RecipeManager.class);
        EasyMock.expect(RecipeManager.getInstance()).andReturn(recipeManager).anyTimes();
        EasyMock.expect(recipeManager.getRecipe("CreateSubscription", profile)).andReturn(recipe).anyTimes();

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setId("12345");
        task.setReferenceId("12345");
        EasyMock.expect(database.createTask(EasyMock.anyObject())).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService taskService = new TaskWebService();
        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\","
                + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,\"createdDate\":1516088453778,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,\"owner\":\"srm-dev\","
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";
        Response response = taskService.createTask(request, content, false);
        Assert.assertEquals(400, response.getStatus());
    }

    @Test
    public void testCreateTaskException() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        EasyMock.expect(request.getRequestURI()).andReturn("http://abc.com");
        EasyMock.expect(request.getHeader(Constants.TRANSACTION_ID_HEADER)).andReturn("71d29b88-9ee2-4e59-9214-fef72b8b07da").anyTimes();

        PowerMock.mockStatic(RecipeManager.class);
        RecipeManager recipeManager = PowerMock.createMock(RecipeManager.class);
        EasyMock.expect(RecipeManager.getInstance()).andReturn(recipeManager).anyTimes();
        EasyMock.expect(recipeManager.getRecipe("RemoveSubscriptions", profile)).andReturn(null).anyTimes();

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        EasyMock.expect(database.createTask(EasyMock.anyObject())).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService taskService = new TaskWebService();
        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\"," + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,\"owner\":\"srm-dev\","
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";
        Response response = taskService.createTask(request, content, false);
        Assert.assertEquals(400, response.getStatus());
    }

    @Test
    public void testCreateTaskDbException() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("srm-test").anyTimes();

        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        EasyMock.expect(request.getHeader(Constants.TRANSACTION_ID_HEADER)).andReturn("71d29b88-9ee2-4e59-9214-fef72b8b07da").anyTimes();
        EasyMock.expect(request.getRequestURI()).andReturn("http://abc.com");

        PowerMock.mockStatic(RecipeManager.class);
        RecipeManager recipeManager = PowerMock.createMock(RecipeManager.class);
        EasyMock.expect(RecipeManager.getInstance()).andReturn(recipeManager).anyTimes();
        Recipe recipe = PowerMock.createMock(Recipe.class);
        EasyMock.expect(recipeManager.getRecipe("RemoveSubscriptions", profile)).andReturn(recipe).anyTimes();

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.createTask(EasyMock.anyObject())).andThrow(new DbException("error"));

        PowerMock.mockStatic(EnvironmentUtilities.class);
        EasyMock.expect(EnvironmentUtilities.getSecret(EasyMock.anyString(), EasyMock.anyString())).andReturn("0123456789test12")
                .anyTimes();

        PowerMock.replayAll(DbFactory.class, database, EnvironmentUtilities.class, Profile.class);
        TaskWebService taskService = new TaskWebService();
        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\"," + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,\"owner\":\"srm-dev\","
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";
        Response response = taskService.createTask(request, content, false);
        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public void testEditTask() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();

        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        Task task = new Task();
        task.setOwner("12345");
        task.setStatus(Status.BLOCKED);
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTask("12345")).andReturn(task).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();

        PowerMock.mockStatic(EnvironmentUtilities.class);
        EasyMock.expect(EnvironmentUtilities.getSecret(EasyMock.anyString(), EasyMock.anyString())).andReturn("0123456789test12")
                .anyTimes();
        PowerMock.mockStatic(Executor.class);
        Executor executor = PowerMock.createMock(Executor.class);
        EasyMock.expect(Executor.getInstance()).andReturn(executor).anyTimes();
        Map<String, String> taskIds = new HashMap<>();
        taskIds.put("1", "4");
        EasyMock.expect(executor.getAllTaskIdsByReferenceId(database, task)).andReturn(taskIds).anyTimes();
        executor.asyncExecuteTask("1", "4", com.ibm.digital.mp.nestor.tasks.Status.QUEUED.toString());
        EasyMock.expectLastCall();
        PowerMock.replayAll(DbFactory.class, database, EnvironmentUtilities.class, Profile.class, Executor.class, executor);
        TaskWebService taskService = new TaskWebService();
        String content = "{\"status\":\"COMPLETED\"}";
        Response response = taskService.editTask(request, content, "12345");
        Assert.assertEquals(200, response.getStatus());

        response = taskService.editTask(request, content, "329_@98");
        Assert.assertEquals(400, response.getStatus());
    }

    @Test
    public void testEditTaskWrongOwner() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12346");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile).anyTimes();

        Task task = new Task();
        task.setOwner("12345");
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTask("12346")).andReturn(task).times(1);
        EasyMock.expect(database.getTask("12346")).andReturn(null).times(1);
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService taskService = new TaskWebService();
        String content = "{\"status\":\"COMPLETED\"}";
        Response response = taskService.editTask(request, content, "12346");
        Assert.assertEquals(404, response.getStatus());
        Assert.assertEquals(404, taskService.editTask(request, content, "12346").getStatus());
    }

    @Test
    public void testEditRunningTask() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        Task task = new Task();
        task.setOwner("12345");
        task.setStatus(Status.RUNNING);
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTask("12345")).andReturn(task).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService taskService = new TaskWebService();
        String content = "{\"status\":\"RUNNING\"}";
        Response response = taskService.editTask(request, content, "12345");
        Assert.assertEquals(409, response.getStatus());
    }

    @Test
    public void testEditTaskException() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        Task task = new Task();
        task.setOwner("12345");
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTask("12345")).andReturn(task).anyTimes();
        EasyMock.expect(database.updateTask(task)).andThrow(new DbException("error"));

        PowerMock.mockStatic(EnvironmentUtilities.class);
        EasyMock.expect(EnvironmentUtilities.getSecret(EasyMock.anyString(), EasyMock.anyString())).andReturn("0123456789test12")
                .anyTimes();

        PowerMock.replayAll(DbFactory.class, database, EnvironmentUtilities.class);
        TaskWebService taskService = new TaskWebService();
        String content = "{\"status\":\"COMPLETED\"}";
        Response response = taskService.editTask(request, content, "12345");
        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public void testEditTaskInvalidEdit() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        Task task = new Task();
        task.setOwner("12345");
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTask("12345")).andReturn(task).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();

        PowerMock.mockStatic(EnvironmentUtilities.class);
        EasyMock.expect(EnvironmentUtilities.getSecret(EasyMock.anyString(), EasyMock.anyString())).andReturn("0123456789test12")
                .anyTimes();

        PowerMock.replayAll(DbFactory.class, database, EnvironmentUtilities.class);
        TaskWebService taskService = new TaskWebService();
        String content = "{\"status\":\"FAILED\"}";
        Response response = taskService.editTask(request, content, "12345");
        Assert.assertEquals(400, response.getStatus());
    }

    @Test
    public void testEditTaskConflict() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        Task task = new Task();
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTask("12345")).andReturn(task).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService taskService = new TaskWebService();
        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\","
                + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,\"createdDate\":1516088453778,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,\"owner\":\"srm-dev\","
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";
        Response response = taskService.editTask(request, content, "12345");
        Assert.assertEquals(404, response.getStatus());
    }

    @Test
    public void testGetTask() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        Task task = new Task();
        task.setOwner("12345");
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTask("12345")).andReturn(task).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService taskService = new TaskWebService();
        Response response = taskService.getTask(request, "12345");
        Assert.assertEquals(200, response.getStatus());

        response = taskService.getTask(null, "329_@98");
        Assert.assertEquals(400, response.getStatus());
    }

    @Test
    public void testGetTaskException() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        Task task = new Task();
        task.setOwner("12345");
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTask("12345")).andThrow(new DbException("err"));
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService taskService = new TaskWebService();
        Response response = taskService.getTask(request, "12345");
        Assert.assertEquals(404, response.getStatus());
    }

    @Test
    public void testGetTaskNotfound() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        Task task = new Task();
        task.setOwner("456");
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTask("12345")).andReturn(task).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService taskService = new TaskWebService();
        Response response = taskService.getTask(request, "12345");
        Assert.assertEquals(404, response.getStatus());
    }

    @Test
    public void testGetTaskExceptionNotfound() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        Task task = new Task();
        task.setOwner("456");
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTask("12345")).andThrow(new DbException("error"));
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService taskService = new TaskWebService();
        Response response = taskService.getTask(request, "12345");
        Assert.assertEquals(404, response.getStatus());
    }

    @Test
    public void testGetTasks() throws DbException
    {
        Task task = new Task();
        List<Task> listTasks = new ArrayList<>();
        listTasks.add(task);
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();
        AllTasks allTasks = PowerMock.createMock(AllTasks.class);
        EasyMock.expect(allTasks.getDocs()).andReturn(listTasks).anyTimes();
        EasyMock.expect(allTasks.getBookmark()).andReturn("bookmark").anyTimes();
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getAllTasks(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyBoolean(), EasyMock.anyBoolean()))
                .andReturn(allTasks).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        JsonObject query = new JsonObject();
        JsonObject selector = new JsonObject();
        String ids = "id";
        selector.addProperty("id", ids);
        query.add("selector", selector);
        TaskWebService tws1 = new TaskWebService();
        Response response = tws1.getTasks(request, query, 0, false, false);
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testGetTasksMissingPayload() throws DbException
    {
        Task task = new Task();
        List<Task> listTasks = new ArrayList<>();
        listTasks.add(task);
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();
        AllTasks allTasks = PowerMock.createMock(AllTasks.class);
        EasyMock.expect(allTasks.getDocs()).andReturn(listTasks).anyTimes();
        EasyMock.expect(allTasks.getBookmark()).andReturn("bookmark").anyTimes();
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getAllTasks(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyBoolean(), EasyMock.anyBoolean()))
                .andReturn(allTasks).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();

        PowerMock.mockStatic(EnvironmentUtilities.class);
        EasyMock.expect(EnvironmentUtilities.getSecret(EasyMock.anyString(), EasyMock.anyString())).andReturn("0123456789test12")
                .anyTimes();

        PowerMock.replayAll(DbFactory.class, database, EnvironmentUtilities.class);
        JsonObject query = new JsonObject();
        JsonObject selector = new JsonObject();
        String ids = "id";
        selector.addProperty("id", ids);
        selector.addProperty("owner", ids);
        query.add("selector", selector);
        TaskWebService tws1 = new TaskWebService();
        Response response = tws1.getTasks(request, query, 0, false, false);
        Assert.assertEquals(400, response.getStatus());
    }

    @Test
    public void testGetTasksException() throws DbException
    {
        Task task = new Task();
        List<Task> listTasks = new ArrayList<>();
        listTasks.add(task);
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        AllTasks allTasks = PowerMock.createMock(AllTasks.class);
        EasyMock.expect(allTasks.getDocs()).andReturn(listTasks).anyTimes();
        EasyMock.expect(allTasks.getBookmark()).andReturn("bookmark").anyTimes();
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        JsonObject query = new JsonObject();
        int pageSize = 1;
        EasyMock.expect(database.getAllTasks(query, pageSize, false, false)).andReturn(allTasks).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService tws1 = new TaskWebService();
        Response response = tws1.getTasks(request, null, 0, false, false);
        Assert.assertEquals(400, response.getStatus());
    }

    @Test
    public void testGetTasksWithQuery() throws DbException
    {
        Task task = new Task();
        List<Task> listTasks = new ArrayList<>();
        listTasks.add(task);
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();
        AllTasks allTasks = PowerMock.createMock(AllTasks.class);
        EasyMock.expect(allTasks.getDocs()).andReturn(listTasks).anyTimes();
        EasyMock.expect(allTasks.getBookmark()).andReturn("bookmark").anyTimes();
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        JsonObject query = new JsonObject();
        query.addProperty("bookmark", "bookmark");
        query.addProperty("sort", "sort");
        EasyMock.expect(database.getAllTasks(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyBoolean(), EasyMock.anyBoolean()))
                .andReturn(allTasks).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService tws1 = new TaskWebService();
        Response response = tws1.getTasks(request, query, 0, false, false);
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testGetTasksBadRequest() throws DbException
    {
        Task task = new Task();
        List<Task> listTasks = new ArrayList<>();
        listTasks.add(task);
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();
        AllTasks allTasks = PowerMock.createMock(AllTasks.class);
        EasyMock.expect(allTasks.getDocs()).andReturn(listTasks).anyTimes();
        EasyMock.expect(allTasks.getBookmark()).andReturn("bookmark").anyTimes();
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        JsonObject query = new JsonObject();
        query.addProperty("selector", "selector");
        query.addProperty("bookmark", "bookmark");
        query.addProperty("sort", "sort");
        int pageSize = 1;
        EasyMock.expect(database.getAllTasks(query, pageSize, false, false)).andReturn(allTasks).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService tws1 = new TaskWebService();
        Response response = tws1.getTasks(request, query, 1, false, false);
        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetTasksSuccess() throws DbException
    {
        Task task = new Task();
        List<Task> listTasks = new ArrayList<>();
        listTasks.add(task);
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();
        AllTasks allTasks = PowerMock.createMock(AllTasks.class);
        EasyMock.expect(allTasks.getDocs()).andReturn(listTasks).anyTimes();
        EasyMock.expect(allTasks.getBookmark()).andReturn("bookmark").anyTimes();
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        JsonObject query = new JsonObject();
        query.add("selector", new JsonObject());
        query.addProperty("bookmark", "bookmark");
        query.addProperty("sort", "sort");
        int pageSize = 1;
        EasyMock.expect(database.getAllTasks(query, pageSize, false, false)).andReturn(allTasks).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService tws1 = new TaskWebService();
        Response response = tws1.getTasks(request, query, 1, false, false);
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testGetCountByIndex() throws DbException
    {

        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        TaskResult tqr = new TaskResult(null, "", 2);
        tqr.setTaskJsonList(new ArrayList<JsonObject>());
        String str = "referenceId:" + "\"" + "cdf" + "\"" + " AND owner:" + "\"" + "12345" + "\"";
        EasyMock.expect(database.getResultByIndexQuery("tasks/byReferenceId", str, "[\"referenceId<string>\"]", false, 200, null))
                .andReturn(tqr).anyTimes();
        PowerMock.replayAll();
        TaskWebService tws1 = new TaskWebService();
        Response response = tws1.getTaskCountByReferenceId(request, "cdf");
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testGetCountByIndexExceptionValidate() throws DbException
    {

        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();

        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);

        TaskResult tqr = new TaskResult(null, "", 2);
        tqr.setTaskJsonList(new ArrayList<JsonObject>());
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        String str = "referenceId:" + "\"" + "cdf" + "\"" + " AND owner:" + "\"" + "12345" + "\"";
        EasyMock.expect(database.getResultByIndexQuery("tasks/byReferenceId", str, null, false, 200, null)).andReturn(tqr).anyTimes();
        PowerMock.replayAll();
        TaskWebService tws1 = new TaskWebService();
        Response response = tws1.getTaskCountByReferenceId(request, null);
        Assert.assertEquals(400, response.getStatus());
    }

    @Test
    public void testGetCountByIndexDbException() throws DbException
    {

        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();

        String str = "referenceId:" + "\"" + "cdf" + "\"" + " AND owner:" + "\"" + "12345" + "\"";
        EasyMock.expect(database.getResultByIndexQuery("tasks/byReferenceId", str, "[\"referenceId<string>\"]", false, 200, null))
                .andThrow(new DbException("")).anyTimes();
        PowerMock.replayAll();
        TaskWebService tws1 = new TaskWebService();
        Response response = tws1.getTaskCountByReferenceId(request, "cdf");
        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public void testValidateTaskId()
    {
        TaskWebService tws = new TaskWebService();
        try
        {
            Response response = tws.executeTask(null, "23f_+2", "1.0", "COMPLETED");
            Assert.assertEquals(400, response.getStatus());
        }
        catch (Exception ex)
        {
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void testGetTaskNotfoundNullTaskNoOwner() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        Task task = null;
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTask("12345")).andReturn(task).anyTimes();
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebService taskService = new TaskWebService();
        Response response = taskService.getTask(request, "12345");
        Assert.assertEquals(404, response.getStatus());
    }
    
    @Test
    public void testEditTaskInvalidSecret() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12346").anyTimes();
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile).anyTimes();

        PowerMock.mockStatic(EnvironmentUtilities.class);
        EasyMock.expect(EnvironmentUtilities.getSecret(EasyMock.anyString(), EasyMock.anyString())).andReturn("01234^789test12")
                .anyTimes();

        Task task = new Task();
        task.setOwner("12346");
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTask("12346")).andReturn(task).times(1);
        EasyMock.expect(database.getTask("12346")).andReturn(null).times(1);
        EasyMock.expect(database.updateTask(task)).andReturn(task).anyTimes();
        PowerMock.replayAll();
        TaskWebService taskService = new TaskWebService();
        String content = "{\"status\":\"COMPLETED\"}";
        Response response = taskService.editTask(request, content, "12346");
        System.out.println("here.... " + response.getStatus());
        Assert.assertEquals(400, response.getStatus());
    }
}
