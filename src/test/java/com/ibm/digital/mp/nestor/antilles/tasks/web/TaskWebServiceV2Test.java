package com.ibm.digital.mp.nestor.antilles.tasks.web;

import java.util.ArrayList;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.antilles.tasks.Executor;
import com.ibm.digital.mp.nestor.client.AntillesClient;
import com.ibm.digital.mp.nestor.config.EnvironmentUtilities;
import com.ibm.digital.mp.nestor.db.Database;
import com.ibm.digital.mp.nestor.db.DbException;
import com.ibm.digital.mp.nestor.db.DbFactory;
import com.ibm.digital.mp.nestor.db.TaskResult;
import com.ibm.digital.mp.nestor.internal.antilles.tasks.recipes.RecipeManager;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.util.JsonUtilities;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ AntillesClient.class, Executor.class, DbFactory.class, Database.class, RecipeManager.class, EnvironmentUtilities.class })
@PowerMockIgnore({ "javax.management.*", "javax.crypto.*" })
public class TaskWebServiceV2Test
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
        TaskWebServiceV2 taskService = new TaskWebServiceV2();
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
        TaskWebServiceV2 taskService = new TaskWebServiceV2();
        Response response = taskService.check();
        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetTasksByIndex() throws DbException
    {
        TaskResult tr = new TaskResult(null, "", 2);
        tr.setTaskJsonList(new ArrayList<JsonObject>());
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();
        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        EasyMock.expect(database.getResultByIndexQuery(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyString(),
                EasyMock.anyBoolean(), EasyMock.anyInt(), EasyMock.anyString())).andReturn(tr).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);

        JsonObject query = new JsonObject();
        query.addProperty("searchIndexId", "indexForGetTasks");
        query.addProperty("sort", "asc");
        query.addProperty("query", "payload.subscriptionId:501598541");
        TaskWebServiceV2 tws1 = new TaskWebServiceV2();
        Response response = tws1.getTasksByIndex(request, query);
        System.out.println("resp " + response.getEntity());
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testGetTasksByIndexPayloadValidation() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();

        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile).anyTimes();

        TaskResult tr = new TaskResult(null, "", 2);
        tr.setTaskJsonList(new ArrayList<JsonObject>());

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getResultByIndexQuery(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyString(),
                EasyMock.anyBoolean(), EasyMock.anyInt(), EasyMock.anyString())).andReturn(tr).anyTimes();

        PowerMock.replayAll();

        Gson gson = new Gson();
        String payloadString = "{}";
        JsonObject inputPyalod = gson.fromJson(payloadString, JsonObject.class);
        TaskWebServiceV2 tws1 = new TaskWebServiceV2();
        Response response = tws1.getTasksByIndex(request, inputPyalod);
        Assert.assertEquals(400, response.getStatus());

        payloadString = "{\"searchIndexId\":null}";
        inputPyalod = gson.fromJson(payloadString, JsonObject.class);
        tws1 = new TaskWebServiceV2();
        response = tws1.getTasksByIndex(request, inputPyalod);
        Assert.assertEquals(400, response.getStatus());

        payloadString = "{\"searchIndexId\":\"indexForGetTasks\"}";
        inputPyalod = gson.fromJson(payloadString, JsonObject.class);
        tws1 = new TaskWebServiceV2();
        response = tws1.getTasksByIndex(request, inputPyalod);
        Assert.assertEquals(400, response.getStatus());

        payloadString = "{\"searchIndexId\":\"indexForGetTasks\", \"query\":null}";
        inputPyalod = gson.fromJson(payloadString, JsonObject.class);
        tws1 = new TaskWebServiceV2();
        response = tws1.getTasksByIndex(request, inputPyalod);
        Assert.assertEquals(400, response.getStatus());

        payloadString = "{\"searchIndexId\":\"indexForGetTasks\", \"query\":\"taskId:sdf2324\"}";
        inputPyalod = gson.fromJson(payloadString, JsonObject.class);
        tws1 = new TaskWebServiceV2();
        response = tws1.getTasksByIndex(request, inputPyalod);
        Assert.assertEquals(200, response.getStatus());

        payloadString = "{\"searchIndexId\":\"indexForGetTasks\", \"query\":\"taskId:sdf2324\","
                + "\"sort\":null,\"includeDocs\":null,\"pageSize\":null,\"bookmark\":null}";
        inputPyalod = gson.fromJson(payloadString, JsonObject.class);
        tws1 = new TaskWebServiceV2();
        response = tws1.getTasksByIndex(request, inputPyalod);
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testGetTasksByIndexMissingPayload() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();

        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        TaskResult tr = new TaskResult(null, "", 2);
        tr.setTaskJsonList(new ArrayList<JsonObject>());

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getResultByIndexQuery(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyString(),
                EasyMock.anyBoolean(), EasyMock.anyInt(), EasyMock.anyString())).andReturn(tr).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);

        JsonObject query = new JsonObject();
        query.addProperty("searchIndexId", "indexForGetTasks");
        query.addProperty("sort", "asc");
        query.addProperty("pageSize", 20);
        query.addProperty("bookmark", "g2wAAAABaANkACxkYmNvcmVAZGIx");
        query.addProperty("includeDocs", true);
        TaskWebServiceV2 tws1 = new TaskWebServiceV2();
        Response response = tws1.getTasksByIndex(request, query);
        Assert.assertEquals(400, response.getStatus());
    }

    @Test
    public void testGetTasksByIndexSuccess() throws DbException
    {
        TaskResult tr = new TaskResult(null, "", 2);
        tr.setTaskJsonList(new ArrayList<JsonObject>());
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getResultByIndexQuery(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyString(),
                EasyMock.anyBoolean(), EasyMock.anyInt(), EasyMock.anyString())).andReturn(tr).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebServiceV2 tws1 = new TaskWebServiceV2();
        JsonObject query = new JsonObject();
        query.addProperty("searchIndexId", "indexForGetTasks");
        query.addProperty("sort", "asc");
        query.addProperty("pageSize", 20);
        query.addProperty("query", "taskId:sdf2324");
        query.addProperty("bookmark", "g2wAAAABaANkACxkYmNvcmVAZGIx");
        query.addProperty("includeDocs", true);
        Response response = tws1.getTasksByIndex(request, query);
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testGetTasksByIndexException() throws DbException
    {
        TaskResult tr = new TaskResult(null, "", 2);
        tr.setTaskJsonList(new ArrayList<JsonObject>());
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getResultByIndexQuery(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyString(),
                EasyMock.anyBoolean(), EasyMock.anyInt(), EasyMock.anyString())).andReturn(tr).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebServiceV2 tws1 = new TaskWebServiceV2();
        Response response = tws1.getTasksByIndex(request, null);
        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetDependentTasks_ExceptionWithEmptyViewName() throws DbException
    {
        TaskResult tr = new TaskResult(null, "", 2);
        tr.setTaskJsonList(new ArrayList<JsonObject>());
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        TaskWebServiceV2 tws1 = new TaskWebServiceV2();
        Response response = tws1.getDependentTasks(request, "111111");
        Assert.assertEquals(500, response.getStatus());

        response = tws1.getDependentTasks(request, "111111");
        Assert.assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetDependentTasks() throws DbException
    {
        String dependentTask = "{\n" + "  \"_id\": \"1\",\n" + "  \"createdDate\": 1568593165175,\n" + "  \"status\": \"QUEUED\",\n"
                + "  \"referenceId\": \"A\",\n" + "  \"parentReferenceId\": \"B\",\n" + "  \"blockedTaskList\": [\n" + "    {\n"
                + "      \"_id\": \"2\",\n" + "      \"createdDate\": 1542329589237,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"B\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"3\",\n" + "      \"createdDate\": 1542329589238,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"4\",\n" + "      \"createdDate\": 1542329589239,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"B\",\n" + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"5\",\n" + "      \"createdDate\": 1542329589240,\n" + "      \"status\": \"BLOCKED\",\n"
                + "      \"referenceId\": \"C\"\n" + "    }\n" + "  ]\n" + "}";
        JsonObject taskPaylod = JsonUtilities.fromJson(dependentTask, JsonObject.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(database.getDependentTasks(EasyMock.anyString())).andReturn(taskPaylod).anyTimes();
        TaskResult tr = new TaskResult(null, "", 2);
        tr.setTaskJsonList(new ArrayList<JsonObject>());
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345");
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        PowerMock.replayAll();
        TaskWebServiceV2 tws1 = new TaskWebServiceV2()
        {
            @Override
            protected Database getDatabase()
            {
                return database;
            }
        };
        Response response = tws1.getDependentTasks(request, "111111");

        Assert.assertEquals(200, response.getStatus());
    }
    
    @Test
    public void testGetTasksCountSuccess() throws DbException
    {
        Profile profile = PowerMock.createMock(Profile.class);
        EasyMock.expect(profile.getId()).andReturn("12345").anyTimes();
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getTaskCountByQuery(EasyMock.anyObject())).andReturn(1L).anyTimes();
        PowerMock.replayAll(DbFactory.class, database);
        TaskWebServiceV2 tws1 = new TaskWebServiceV2();
        JsonObject query = new JsonObject();
        query.addProperty("status", "NEW");
        Response response = tws1.getTasksCount(request, query);
        Assert.assertEquals(200, response.getStatus());
    }
}
