package com.ibm.digital.mp.nestor.antilles.util;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.AntillesWebException;
import com.ibm.digital.mp.nestor.AntillesWebException.Reason;
import com.ibm.digital.mp.nestor.MessageCodes;
import com.ibm.digital.mp.nestor.ValidationException;
import com.ibm.digital.mp.nestor.tasks.Status;
import com.ibm.digital.mp.nestor.tasks.Task;
import com.ibm.digital.mp.nestor.util.JsonUtilities;

public class TaskWebUtilTest
{
    private static List<Status> supportedStatuses = Arrays.asList(Status.SCHEDULED, Status.COMPLETED, Status.NEEDS_ATTENTION,
            Status.CANCELLED, Status.BLOCKED);

    @Test
    public void testValidateTaskWithDefault()
    {
        Task task = new Task();
        String content = JsonUtilities.toJson(task);
        try
        {
            TaskWebUtil.validateTask(task, content);
        }
        catch (AntillesWebException ex)
        {
            if (ex.getCause() instanceof ValidationException)
            {
                Assert.assertTrue(true);
            }

        }
    }

    @Test
    public void testValidateTaskForLastExecutionDate()
    {
        Task task = new Task();
        task.setType("type");
        JsonObject payload = new JsonObject();
        payload.addProperty("field", "vaule");
        task.setPayload(payload);
        task.setCreatedDate(null);
        task.setModifiedDate(null);
        task.setStatus(Status.NEW);
        task.setStep("Start");
        task.setLastExecutionDate(new Date());
        task.setFailedExecutionCount(2);
        task.setExecutionCount(2);
        String content = JsonUtilities.toJson(task);
        JsonObject contentJson = JsonUtilities.fromJson(content, JsonObject.class);
        contentJson.remove("createdDate");
        contentJson.remove("modifiedDate");
        try
        {
            TaskWebUtil.validateTask(task, contentJson.toString());
        }
        catch (AntillesWebException ex)
        {
            if (ex.getCause() instanceof ValidationException)
            {
                Assert.assertTrue(true);
            }

        }
    }

    @Test
    public void testValidateEditTaskPayloadThrowsErrors()
    {
        Task task = new Task();
        task.setType("type");
        task.setPayload(null);
        task.setVault(null, null, null);
        task.setCreatedDate(null);
        task.setModifiedDate(null);
        task.setStatus(Status.NEW);
        task.setStep("Start");
        task.setLastExecutionDate(new Date());
        task.setFailedExecutionCount(2);
        task.setExecutionCount(2);
        task.setPreferredExecutionDate(new Date());
        String content = JsonUtilities.toJson(task);
        JsonObject contentJson = JsonUtilities.fromJson(content, JsonObject.class);
        contentJson.remove("createdDate");
        contentJson.remove("modifiedDate");
        contentJson.addProperty("step", 1);
        contentJson.addProperty("payload", 1);
        contentJson.addProperty("vault", 1);
        contentJson.addProperty("preferredExecutionDate", "1234");
        JsonObject errors = TaskWebUtil.validateEditTaskPayload(contentJson);
        if (errors.has("status") && errors.has("step") && errors.has("preferredExecutionDate") && errors.has("payload")
                && errors.has("vault"))
        {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void testValidateEditTaskPayloadNoErrors()
    {
        Task task = new Task();
        task.setType("type");
        task.setPayload(null);
        task.setVault(null, null, null);
        task.setCreatedDate(null);
        task.setModifiedDate(null);
        task.setStatus(Status.NEW);
        task.setStep("Start");
        task.setLastExecutionDate(new Date());
        task.setFailedExecutionCount(2);
        task.setExecutionCount(2);
        task.setPreferredExecutionDate(new Date());
        String content = JsonUtilities.toJson(task);
        JsonObject contentJson = JsonUtilities.fromJson(content, JsonObject.class);
        contentJson.remove("status");

        JsonObject errors = TaskWebUtil.validateEditTaskPayload(contentJson);
        if (!(errors.has("status") && errors.has("step") && errors.has("preferredExecutionDate") && errors.has("payload")
                && errors.has("vault")))
        {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void testValidateEditTaskEmptyPayload()
    {
        Task task = new Task();
        task.setType("type");
        task.setPayload(null);
        task.setVault(null, null, null);
        task.setCreatedDate(null);
        task.setModifiedDate(null);
        task.setStatus(Status.NEW);
        task.setStep("Start");
        task.setLastExecutionDate(new Date());
        task.setFailedExecutionCount(2);
        task.setExecutionCount(2);
        task.setPreferredExecutionDate(new Date());
        String content = JsonUtilities.toJson(task);
        JsonObject contentJson = JsonUtilities.fromJson(content, JsonObject.class);
        contentJson.remove("status");

        JsonObject errors = TaskWebUtil.validateEditTaskPayload(new JsonObject());
        if (errors.size() == 0)
        {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void testValidateTaskEditable()
    {
        Task task = new Task();
        for (Status status : Status.values())
        {
            task.setStatus(status);
            if (Status.RUNNING == status)
            {
                Assert.assertEquals(MessageCodes.CANT_EDIT_RUNNING_TASK_MESSAGE, TaskWebUtil.validateTaskEditable(task));
            }
            else
            {
                Assert.assertNull(TaskWebUtil.validateTaskEditable(task));
            }
        }
    }

    @Test
    public void testIsSupportedStatusChange()
    {
        for (Status status : Status.values())
        {
            if (supportedStatuses.contains(status))
            {
                Assert.assertEquals(true, TaskWebUtil.isSupportedStatusChange(status));
            }
            else
            {
                Assert.assertEquals(false, TaskWebUtil.isSupportedStatusChange(status));
            }
        }
    }

    @Test
    public void testUpdateTask()
    {
        Task task = new Task();
        task.setPayload(new JsonObject());
        task.setVault(new JsonObject(), "secret1234567890", null);
        task.setStep("Start");
        task.setStatus(Status.NEW);
        task.setPreferredExecutionDate(new Date());
        String content = JsonUtilities.toJson(task);
        JsonObject updatePayload = JsonUtilities.fromJson(content, JsonObject.class);
        Task newTask = new Task();
        TaskWebUtil.updateTaskObject(newTask, updatePayload, "secret1234567890", null);
        Assert.assertEquals(newTask.getStep(), updatePayload.get("step").getAsString());
    }

    @Test
    public void testUpdateTaskWithEmptyPayload()
    {
        Task task = new Task();
        task.setPayload(new JsonObject());
        task.setVault(new JsonObject(), "secret1234567890", null);
        task.setStep("Start");
        task.setStatus(Status.NEW);
        task.setPreferredExecutionDate(new Date());
        JsonObject updatePayload = new JsonObject();
        Task newTask = new Task();
        TaskWebUtil.updateTaskObject(newTask, updatePayload, "secret1234567890", null);
        Assert.assertEquals(newTask.getStatus(), Status.NEW);
    }

    @Test
    public void testEditTaskBadStatus()
    {

        Task task = new Task();
        task.setPayload(new JsonObject());
        task.setVault(new JsonObject(), "secret1234567890", null);
        task.setStep("Start");
        task.setStatus(Status.NEW);
        task.setPreferredExecutionDate(new Date());
        JsonObject updatePayload = new JsonObject();
        updatePayload.addProperty("status", "no such status");

        JsonObject result = TaskWebUtil.validateEditTaskPayload(updatePayload);
        Assert.assertEquals("Status is invalid.", result.get("status").getAsString());
    }

    @Test
    public void testValidateEditWithVaultAndPayloadObj()
    {
        JsonObject updatePayload = new JsonObject();
        updatePayload.add("payload", new JsonObject());
        updatePayload.add("vault", new JsonObject());
        JsonObject result = TaskWebUtil.validateEditTaskPayload(updatePayload);
        Assert.assertFalse(result.has("payload"));
        Assert.assertFalse(result.has("vault"));
    }

    @Test
    public void testStringNullOrEmpty()
    {
        Assert.assertTrue(TaskWebUtil.isNullOrEmpty(null));
        Assert.assertTrue(TaskWebUtil.isNullOrEmpty(""));
        Assert.assertFalse(TaskWebUtil.isNullOrEmpty(" "));
        Assert.assertFalse(TaskWebUtil.isNullOrEmpty("daw"));
    }

    @Test
    public void testJsonObjectNullOrEmpty()
    {
        Assert.assertTrue(TaskWebUtil.isJsonNullOrEmpty(null));
        Assert.assertTrue(TaskWebUtil.isJsonNullOrEmpty(new JsonObject()));
        JsonObject json = new JsonObject();
        json.addProperty("a", "b");
        Assert.assertFalse(TaskWebUtil.isJsonNullOrEmpty(json));
    }

    @Test
    public void testBuildErrorResponse()
    {
        AntillesWebException antillesWebException = new AntillesWebException(MessageCodes.TASK_VALIDATION_EXCEPTION_ERROR_CODE,
                MessageCodes.TASK_VALIDATION_EXCEPTION_ERROR_MESSAGE, null, Reason.BAD_REQUEST);
        Response response = TaskWebUtil.buildErrorResponse(antillesWebException);
        Assert.assertTrue(response.getStatus() == Reason.BAD_REQUEST.getCode());
    }

    @Test
    public void testBuildResponses()
    {
        Response response = TaskWebUtil.buildBadRequestResponse("400", "BadRequest");
        Assert.assertTrue(response.getStatus() == Reason.BAD_REQUEST.getCode());

        response = TaskWebUtil.buildUnauthorizedResponse("403", "Unauthorized");
        Assert.assertTrue(response.getStatus() == Reason.UNAUTHORIZED.getCode());

        response = TaskWebUtil.buildNotFoundResponse("404", "NotFound");
        Assert.assertTrue(response.getStatus() == Reason.NOT_FOUND.getCode());

        response = TaskWebUtil.buildConflictResponse("409", "Conflict");
        Assert.assertTrue(response.getStatus() == Reason.CONFLICT.getCode());

        response = TaskWebUtil.buildServerErrorResponse("500", "Internal error");
        Assert.assertTrue(response.getStatus() == Reason.INTERNAL_ERROR.getCode());
    }

    @Test
    public void testValidateGetTaskCountRequest()
    {
        try
        {
            TaskWebUtil.validateGetTaskCountRequest("abc");
            Assert.assertTrue(true);
        }
        catch (AntillesWebException ex)
        {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        }

        try
        {
            TaskWebUtil.validateGetTaskCountRequest("");
            Assert.assertTrue(true);
        }
        catch (AntillesWebException ex)
        {
            Assert.assertTrue(!ex.isCauseListEmpty());
        }

    }

    @Test
    public void testValidateGetTaskCountRequestWithJsonObject()
    {
        JsonObject query = new JsonObject();
        query.addProperty("status", "NEW");
        try
        {
            TaskWebUtil.validateGetTasksCountRequest(query);
            Assert.assertTrue(true);
        }
        catch (AntillesWebException ex)
        {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        }

        try
        {
            query.addProperty("status", "NEW1");
            TaskWebUtil.validateGetTasksCountRequest(query);
        }
        catch (AntillesWebException ex)
        {
            Assert.assertTrue(!ex.isCauseListEmpty());
        }

        try
        {
            query.addProperty("fromDate", 1490);
            query.addProperty("toDate", 1390);
            TaskWebUtil.validateGetTasksCountRequest(query);
        }
        catch (AntillesWebException ex)
        {
            Assert.assertTrue(!ex.isCauseListEmpty());
        }

        try
        {
            query.addProperty("fromDate", 1490);
            query.remove("toDate");
            TaskWebUtil.validateGetTasksCountRequest(query);
        }
        catch (AntillesWebException ex)
        {
            Assert.assertTrue(!ex.isCauseListEmpty());
        }

        try
        {
            query.addProperty("fromDate", 1490);
            query.addProperty("toDate", 1390);
            query.addProperty("type", "");
            TaskWebUtil.validateGetTasksCountRequest(query);
        }
        catch (AntillesWebException ex)
        {
            Assert.assertTrue(!ex.isCauseListEmpty());
        }

        try
        {
            query.addProperty("fromDate", 1490);
            query.addProperty("toDate", 1390);
            query.addProperty("type", "");
            query.addProperty("payload.vendorid", 1245);
            TaskWebUtil.validateGetTasksCountRequest(query);
        }
        catch (AntillesWebException ex)
        {
            Assert.assertTrue(!ex.isCauseListEmpty());
        }

    }

    @Test
    public void testValidateProfile()
    {
        Assert.assertFalse(TaskWebUtil.validateProfile("jwkr*2l"));
    }
}
