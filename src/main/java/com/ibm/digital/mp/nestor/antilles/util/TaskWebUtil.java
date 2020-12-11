package com.ibm.digital.mp.nestor.antilles.util;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.AntillesWebException;
import com.ibm.digital.mp.nestor.AntillesWebException.Reason;
import com.ibm.digital.mp.nestor.MessageCodes;
import com.ibm.digital.mp.nestor.ValidationException;
import com.ibm.digital.mp.nestor.antilles.provider.GsonUtil;
import com.ibm.digital.mp.nestor.antilles.webhooks.WebhookAuthenticationException;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.tasks.Status;
import com.ibm.digital.mp.nestor.tasks.Task;
import com.ibm.digital.mp.nestor.tasks.recipes.util.DateUtilities;
import com.ibm.digital.mp.nestor.util.Constants;
import com.mongodb.client.model.Filters;

public class TaskWebUtil
{
    TaskWebUtil()
    {

    }

    private static List<Status> supportedStatuses = Arrays.asList(Status.SCHEDULED, Status.COMPLETED, Status.NEEDS_ATTENTION,
            Status.CANCELLED, Status.BLOCKED);

    /**
     * Validates the task.
     * 
     * @param task
     *            task
     * @throws AntillesWebException
     *             exception
     */
    public static void validateTask(Task task, String content) throws AntillesWebException
    {
        JsonObject contentJsonObject = new Gson().fromJson(content, JsonObject.class);
        AntillesWebException antillesWebException = new AntillesWebException(MessageCodes.TASK_VALIDATION_EXCEPTION_ERROR_CODE,
                MessageCodes.TASK_VALIDATION_EXCEPTION_ERROR_MESSAGE, null, Reason.BAD_REQUEST);

        if (isNullOrEmpty(task.getType()))
        {
            antillesWebException.setCause(new ValidationException(MessageCodes.TASK_TYPE_REQUIRED_ERROR_CODE,
                    MessageCodes.TASK_REQUIRED_ERROR_MESSAGE.replace("[field]", "type"), null));
        }

        if (isJsonNullOrEmpty(task.getPayload()))
        {
            antillesWebException.setCause(new ValidationException(MessageCodes.TASK_PAYLOAD_REQUIRED_ERROR_CODE,
                    MessageCodes.TASK_REQUIRED_ERROR_MESSAGE.replace("[field]", "payload"), null));
        }

        if (contentJsonObject.has("createdDate"))
        {
            antillesWebException.setCause(new ValidationException(MessageCodes.TASK_CREATED_DATE_NOT_ALLOWED_ERROR_CODE,
                    MessageCodes.TASK_NOT_ALLOWED_FIELD_ERROR_MESSAGE.replace("[field]", "createdDate"), null));
        }

        if (contentJsonObject.has("modifiedDate"))
        {
            antillesWebException.setCause(new ValidationException(MessageCodes.TASK_MODIFIED_DATE_NOT_ALLOWED_ERROR_CODE,
                    MessageCodes.TASK_NOT_ALLOWED_FIELD_ERROR_MESSAGE.replace("[field]", "modifiedDate"), null));
        }

        if (contentJsonObject.has("status"))
        {
            antillesWebException.setCause(new ValidationException(MessageCodes.TASK_STATUS_NOT_ALLOWED_ERROR_CODE,
                    MessageCodes.TASK_NOT_ALLOWED_FIELD_ERROR_MESSAGE.replace("[field]", "status"), null));
        }

        if (contentJsonObject.has("step"))
        {
            antillesWebException.setCause(new ValidationException(MessageCodes.TASK_STEP_NOT_ALLOWED_ERROR_CODE,
                    MessageCodes.TASK_NOT_ALLOWED_FIELD_ERROR_MESSAGE.replace("[field]", "step"), null));
        }

        if (task.getLastExecutionDate() != null)
        {
            antillesWebException.setCause(new ValidationException(MessageCodes.TASK_EXECUTION_DATE_NOT_ALLOWED_ERROR_CODE,
                    MessageCodes.TASK_NOT_ALLOWED_FIELD_ERROR_MESSAGE.replace("[field]", "lastExecutionDate"), null));
        }

        if (task.getExecutionCount() > 0)
        {
            antillesWebException.setCause(new ValidationException(MessageCodes.TASK_EXECUTION_COUNT_NOT_ALLOWED_ERROR_CODE,
                    MessageCodes.TASK_NOT_ALLOWED_FIELD_ERROR_MESSAGE.replace("[field]", "executionCount"), null));
        }

        if (task.getFailedExecutionCount() > 0)
        {
            antillesWebException.setCause(new ValidationException(MessageCodes.TASK_FAILED_EXECUTION_COUNT_NOT_ALLOWED_ERROR_CODE,
                    MessageCodes.TASK_NOT_ALLOWED_FIELD_ERROR_MESSAGE.replace("[field]", "failedExecutionCount"), null));
        }
        if (!antillesWebException.isCauseListEmpty())
        {
            throw antillesWebException;
        }
    }

    /**
     * validate api request for gettaskcount.
     * 
     * @param referneceId
     *            string
     * @throws AntillesWebException
     *             exception
     */
    public static void validateGetTaskCountRequest(String referneceId) throws AntillesWebException
    {
        AntillesWebException antillesWebException = new AntillesWebException(MessageCodes.COUNT_API_VALIDATION_EXCEPTION_ERROR_CODE,
                MessageCodes.API_VALIDATION_EXCEPTION_ERROR_MESSAGE, null, Reason.BAD_REQUEST);

        if (isNullOrEmpty(referneceId))
        {
            antillesWebException.setCause(new ValidationException(MessageCodes.QUERY_NAME_REQUIRED_ERROR_CODE,
                    MessageCodes.REQUIRED_ERROR_MESSAGE.replace("[field]", "referneceId"), null));
        }

        if (!antillesWebException.isCauseListEmpty())
        {
            throw antillesWebException;
        }

    }

    /**
     * Checks if a task is currently editable.
     * 
     * @param task
     *            The task
     * @return An error message or <code>null</code>
     */
    public static String validateTaskEditable(Task task)
    {
        if (Status.RUNNING == task.getStatus())
        {
            return MessageCodes.CANT_EDIT_RUNNING_TASK_MESSAGE;
        }
        return null;
    }

    /**
     * Validates the edit task payload.
     * 
     * @param updatePayload
     *            The updates to the task.
     * @return A {@link JsonObject} with validation errors, or empty if there are none
     */
    public static JsonObject validateEditTaskPayload(JsonObject updatePayload)
    {
        JsonObject errors = new JsonObject();
        JsonObject payload = GsonUtil.deepCopy(updatePayload, JsonObject.class);
        if (payload.has("status"))
        {
            String statusString = payload.get("status").getAsString();
            try
            {
                Status status = Status.valueOf(statusString);
                if (!isSupportedStatusChange(status))
                {
                    errors.addProperty("status", "Supported status values are: " + supportedStatuses);
                }
            }
            catch (Exception ex)
            {
                errors.addProperty("status", "Status is invalid.");
            }
            payload.remove("status");
        }

        if (payload.has("step"))
        {
            if (!payload.getAsJsonPrimitive("step").isString())
            {
                errors.addProperty("step", "The step must be a string.");
            }
            payload.remove("step");
        }

        if (payload.has("preferredExecutionDate"))
        {
            if (!payload.getAsJsonPrimitive("preferredExecutionDate").isNumber())
            {
                errors.addProperty("preferredExecutionDate", "The preferredExecutionDate must be a number.");
            }
            payload.remove("preferredExecutionDate");
        }

        if (payload.has("payload"))
        {
            if (!payload.get("payload").isJsonObject())
            {
                errors.addProperty("payload", "The payload must be an Object.");
            }
            payload.remove("payload");
        }

        if (payload.has("vault"))
        {
            if (!payload.get("vault").isJsonObject())
            {
                errors.addProperty("vault", "The vault must be an Object.");
            }
            payload.remove("vault");
        }

        if (payload.size() > 0)
        {
            errors.addProperty("unsupportedProperties", "The following fields may not be edited " + payload.keySet().toString() + ".");
        }

        return errors;
    }

    /**
     * Validates get task count request.
     * 
     * @param queryJson
     *            JsonObject
     * @throws AntillesWebException
     *             exception
     */
    public static void validateGetTasksCountRequest(JsonObject queryJson) throws AntillesWebException
    {
        AntillesWebException antillesWebException = new AntillesWebException(MessageCodes.TASK_VALIDATION_EXCEPTION_ERROR_CODE,
                MessageCodes.TASK_VALIDATION_EXCEPTION_ERROR_MESSAGE, null, Reason.BAD_REQUEST);
        JsonObject clonedQueryJson = queryJson.deepCopy();
        clonedQueryJson.remove(Constants.STATUS);
        clonedQueryJson.remove(Constants.TYPE);
        clonedQueryJson.remove(Constants.FROM_DATE);
        clonedQueryJson.remove(Constants.TO_DATE);

        if (clonedQueryJson.size() > 0)
        {
            antillesWebException.setCause(new ValidationException(MessageCodes.FIELDS_NOT_ALLOWED_ERROR_CODE,
                    MessageCodes.TASK_NOT_ALLOWED_FIELD_ERROR_MESSAGE.replace("[field]", clonedQueryJson.keySet().toString()), null));
        }
        
        if (queryJson.has(Constants.STATUS))
        {
            try
            {
                Status.valueOf(queryJson.get(Constants.STATUS).getAsString());
            }
            catch (IllegalArgumentException ex)
            {
                antillesWebException.setCause(
                        new ValidationException(MessageCodes.INVALID_STATUS_ERROR_CODE, MessageCodes.INVALID_STATUS_ERROR_MESSAGE, null));
            }
        }
        else
        {
            antillesWebException.setCause(
                    new ValidationException(MessageCodes.MANDATORY_STATUS_ERROR_CODE, MessageCodes.MANDATORY_STATUS_ERROR_MESSAGE, null));
        }

        if (queryJson.has(Constants.FROM_DATE))
        {
            long fromDate = queryJson.get(Constants.FROM_DATE).getAsLong();
            if (fromDate > new Date().getTime())
            {
                antillesWebException.setCause(new ValidationException(MessageCodes.FUTURE_FROM_DATE_ERROR_CODE,
                        MessageCodes.FUTURE_FROM_DATE_ERROR_MESSAGE, null));
            }
        }

        if (queryJson.has(Constants.FROM_DATE) && queryJson.has(Constants.TO_DATE))
        {
            long fromDate = queryJson.get(Constants.FROM_DATE).getAsLong();
            long toDate = queryJson.get(Constants.TO_DATE).getAsLong();
            if (fromDate >= toDate)
            {
                antillesWebException.setCause(new ValidationException(MessageCodes.FROMDATE_GREATERTHAN_TODATE_ERROR_CODE,
                        MessageCodes.FROMDATE_GREATERTHAN_TODATE_ERROR_MESSAGE, null));
            }
        }

        if (queryJson.has(Constants.TO_DATE) && !queryJson.has(Constants.FROM_DATE))
        {
            antillesWebException.setCause(new ValidationException(MessageCodes.TODATE_NOTALLOWED_WITHOUT_FROMDATE_ERROR_CODE,
                    MessageCodes.TODATE_NOTALLOWED_WITHOUT_FROMDATE_ERROR_MESSAGE, null));
        }

        if (!antillesWebException.isCauseListEmpty())
        {
            throw antillesWebException;
        }
    }

    /**
     * Prepares bson query.
     * 
     * @param queryJson
     *            jsonobject
     * @param profile
     *            profile
     * @return List
     */
    public static List<Bson> prepareBsonQuery(JsonObject queryJson, Profile profile)
    {
        List<Bson> filters = new ArrayList<Bson>();

        if (queryJson.has(Constants.STATUS))
        {
            filters.add(Filters.eq(Constants.STATUS, queryJson.get(Constants.STATUS).getAsString().toUpperCase()));
        }
        if (queryJson.has(Constants.TYPE))
        {
            filters.add(Filters.eq(Constants.TYPE, queryJson.get(Constants.TYPE).getAsString()));
        }
        if (queryJson.has(Constants.FROM_DATE))
        {
            filters.add(Filters.gte(Constants.CREATED_DATE, queryJson.get(Constants.FROM_DATE).getAsLong()));
            if (!queryJson.has(Constants.TO_DATE))
            {
                filters.add(Filters.lte(Constants.CREATED_DATE, DateUtilities.getCurrentDateAsLong()));
            }
        }
        if (queryJson.has(Constants.TO_DATE))
        {
            filters.add(Filters.lte(Constants.CREATED_DATE, queryJson.get(Constants.TO_DATE).getAsLong()));
        }

        if (!Constants.SYSTEM_PROFILE_ID.equals(profile.getId()))
        {
            filters.add(Filters.eq(Constants.OWNER, profile.getId()));
        }

        return filters;
    }

    public static boolean isSupportedStatusChange(Status status)
    {
        return supportedStatuses.contains(status);
    }

    /**
     * Updates a {@link Task} object from a given {@link JsonObject}. This method will not validate the payload.
     * 
     * @param taskToUpdate
     *            The original task to update.
     * @param updatePayload
     *            The Json containing the fields to update.
     * @return The updated task object
     */
    public static Task updateTaskObject(Task taskToUpdate, JsonObject updatePayload, String vaultSecret, Logger logger)
    {
        if (updatePayload.has("status"))
        {
            Status status = Status.valueOf(updatePayload.get("status").getAsString());
            taskToUpdate.setStatus(status);
        }

        if (updatePayload.has("step"))
        {
            taskToUpdate.setStep(updatePayload.get("step").getAsString());
        }

        if (updatePayload.has("preferredExecutionDate"))
        {
            taskToUpdate.setPreferredExecutionDate(new Date(updatePayload.get("preferredExecutionDate").getAsLong()));

        }

        if (updatePayload.has("payload"))
        {
            taskToUpdate.setPayload(updatePayload.getAsJsonObject("payload"));
        }

        if (updatePayload.has("vault"))
        {
            taskToUpdate.setVault(updatePayload.getAsJsonObject("vault"), vaultSecret, logger);
        }
        return taskToUpdate;
    }

    protected static boolean isNullOrEmpty(String value)
    {
        if (value == null || value.isEmpty())
        {
            return true;
        }
        return false;
    }

    protected static boolean isJsonNullOrEmpty(JsonObject value)
    {
        if (value == null || value.size() == 0)
        {
            return true;
        }
        return false;
    }

    public static Response buildErrorResponse(String errorCode, String errorMessage, Reason reason)
    {
        AntillesWebException antillesWebException = new AntillesWebException(errorCode, errorMessage, null, reason);
        return buildErrorResponse(antillesWebException);
    }

    public static Response buildErrorResponse(AntillesWebException antillesWebException)
    {
        return Response.status(antillesWebException.getHttpCode()).entity(antillesWebException.getErrorResponse()).build();
    }

    public static Response buildErrorResponse(WebhookAuthenticationException webhookAuthenticationException)
    {
        return Response.status(webhookAuthenticationException.getErrorCode().length()).entity(webhookAuthenticationException.getMessage())
                .build();
    }

    public static Response buildBadRequestResponse(String errorCode, String errorMessage)
    {
        return buildErrorResponse(errorCode, errorMessage, Reason.BAD_REQUEST);
    }

    public static Response buildServerErrorResponse(String errorCode, String errorMessage)
    {
        return buildErrorResponse(errorCode, errorMessage, Reason.INTERNAL_ERROR);
    }

    public static Response buildUnauthorizedResponse(String errorCode, String errorMessage)
    {
        return buildErrorResponse(errorCode, errorMessage, Reason.UNAUTHORIZED);
    }

    public static Response buildNotFoundResponse(String errorCode, String errorMessage)
    {
        return buildErrorResponse(errorCode, errorMessage, Reason.NOT_FOUND);
    }

    public static Response buildConflictResponse(String errorCode, String errorMessage)
    {
        return buildErrorResponse(errorCode, errorMessage, Reason.CONFLICT);
    }

    public static Response buildRedirectionResponse(URI redirectUri)
    {
        return Response.status(Response.Status.MOVED_PERMANENTLY).location(redirectUri).build();
    }

    /**
     * Validate the taskId format.
     * 
     * @param taskId
     *            taskId
     * @return An error message or <code>null</code>
     */
    public static boolean validateTaskId(String taskId)
    {
        boolean isValid = true;
        String pattern = "[a-zA-Z0-9]+";
        if (!taskId.matches(pattern))
        {
            isValid = false;
        }
        return isValid;
    }

    /**
     * Validate given string for alphanumeric and hyphen character.
     * 
     * @param profile
     *            inputString
     * @return An error message or <code>null</code>
     */
    public static boolean validateProfile(String profile)
    {
        boolean isValid = true;
        String pattern = "[a-zA-Z0-9-]+";
        if (!profile.matches(pattern))
        {
            isValid = false;
        }
        return isValid;
    }
}
