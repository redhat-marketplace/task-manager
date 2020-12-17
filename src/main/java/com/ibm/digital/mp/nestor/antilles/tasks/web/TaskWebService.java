/********************************************************** {COPYRIGHT-TOP} ****
 * IBM Internal Use Only
 * IBM Marketplace SaaS Resource Manager
 *
 * (C) Copyright IBM Corp. 2017  All Rights Reserved.
 *
 * The source code for this program is not published or otherwise  
 * divested of its trade secrets, irrespective of what has been 
 * deposited with the U.S. Copyright Office.
 ********************************************************** {COPYRIGHT-END} ***/

package com.ibm.digital.mp.nestor.antilles.tasks.web;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.digital.mp.nestor.AntillesWebException;
import com.ibm.digital.mp.nestor.MessageCodes;
import com.ibm.digital.mp.nestor.antilles.tasks.Executor;
import com.ibm.digital.mp.nestor.antilles.tasks.web.filters.Maintenance;
import com.ibm.digital.mp.nestor.antilles.util.TaskWebUtil;
import com.ibm.digital.mp.nestor.auth.Authorized;
import com.ibm.digital.mp.nestor.config.EnvironmentUtilities;
import com.ibm.digital.mp.nestor.config.NestorConfigurationFactory;
import com.ibm.digital.mp.nestor.db.Database;
import com.ibm.digital.mp.nestor.db.DbException;
import com.ibm.digital.mp.nestor.db.DbFactory;
import com.ibm.digital.mp.nestor.db.TaskResult;
import com.ibm.digital.mp.nestor.internal.antilles.tasks.recipes.RecipeManager;
import com.ibm.digital.mp.nestor.tasks.AllTasks;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.tasks.Task;
import com.ibm.digital.mp.nestor.tasks.recipes.Recipe;
import com.ibm.digital.mp.nestor.util.AesEncodingUtil;
import com.ibm.digital.mp.nestor.util.Constants;
import com.ibm.digital.mp.nestor.util.JsonUtilities;

@Path("/v1/tasks")
public class TaskWebService
{
    private static final Logger logger = LogManager.getLogger(TaskWebService.class);

    /**
     * Sanity check.
     * 
     * @return Response
     */
    @GET
    @Path("/check")
    public Response check()
    {

        try
        {
            // check if the db connection works as well
            // system profile should ALWAYS exist
            getDatabase().getProfile("system");
        }
        catch (DbException ex)
        {
            logger.error(ex.getMessage(), ex);
            return Response.serverError().build();
        }

        return Response.ok("All good!").build();
    }

    /**
     * Create task POST API.
     * 
     * @param request
     *            HttpServletRequest object
     * @param content
     *            input task object
     * @return HttpServletResponse object
     */

    @Authorized(role = "tasks.write")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createTask(@Context HttpServletRequest request, String content,
            @QueryParam("blockTaskExecution") Boolean blockTaskExecution)
    {
        try
        {
            Task task = JsonUtilities.fromJson(content, Task.class);
            logger.info("Received request to create a task of type [" + task.getType() + "].");
            Profile profile = (Profile) request.getAttribute(Constants.PROFILE);
            if (task.getOwner() == null)
            {
                task.setOwner(profile.getId());
            }

            TaskWebUtil.validateTask(task, content);
            // Validate tasks - profile is complete .i.e. notifications etc.

            // If a recipe isnt available for a task, then we should reject it right away
            Recipe recipe = RecipeManager.getInstance().getRecipe(task.getType(), profile);
            if (recipe == null)
            {
                return TaskWebUtil.buildBadRequestResponse(MessageCodes.TASK_TYPE_INVALID_ERROR_CODE,
                        MessageCodes.TASK_TYPE_INVALID_ERROR_MESSAGE);
            }

            if (task.getPreferredExecutionDate() != null)
            {
                task.setStatus(com.ibm.digital.mp.nestor.tasks.Status.SCHEDULED);
            }

            if (!org.apache.commons.lang3.StringUtils.isBlank(ThreadContext.get(Constants.TRANSACTION_LOG_ID)))
            {
                task.setTransactionId(ThreadContext.get(Constants.TRANSACTION_LOG_ID));
            }

            task = getDatabase().createTask(task);
            String taskId = task.getId();
            logger.info("Created task [" + taskId + "].");
            
            // The task is created in the database, ALWAYS return a success from the API now
            // See https://github.ibm.com/digital-marketplace/nestor/issues/699
            
            try
            {
                // Added if condition just to print info message for the duration of maintenance
                if (NestorConfigurationFactory.getInstance().getNestorConfiguration().maintenanceOn())
                {
                    logger.info("Maintenance window is ON. Task execution will be blocked for id {}", taskId);
                }
                
                // Backward compatibility for when blockTaskExecution didnt exist
                if (!NestorConfigurationFactory.getInstance().getNestorConfiguration().maintenanceOn()
                        && (blockTaskExecution == null || !blockTaskExecution))
                {

                    executeTask(request, task.getId(), task.getRevision(), task.getStatus().toString());
                }
            }
            catch (Exception ex)
            {
                logger.error(ex.getMessage(), ex);
            }
            
            // Send a success response
            String requestUri = request.getRequestURI();
            URI taskUri = URI.create(requestUri + "/" + taskId);
            String json = JsonUtilities.toExternalJson(task);
            return Response.created(taskUri).entity(json).build();
        }
        catch (DbException ex)
        {
            logger.error(ex.getMessage(), ex);
            return TaskWebUtil.buildServerErrorResponse(MessageCodes.TASK_TYPE_INVALID_ERROR_CODE,
                    MessageCodes.TASK_TYPE_INVALID_ERROR_MESSAGE);
        }
        catch (AntillesWebException antillesWebException)
        {
            return TaskWebUtil.buildErrorResponse(antillesWebException);
        }
    }

    /**
     * Web API to edit tasks.
     * <ul>
     * <li>status</li>
     * <li>step</li>
     * <li>preferredExecutionDate</li>
     * <li>payload</li>
     * <li>vault</li>
     * </ul>
     * 
     * <p>
     * Web responses
     * <ul>
     * <li>200 - Task edit successful</li>
     * <li>400 - Bad or invalid request</li>
     * <li>401 - User credentials incorrect (Authentication failure)</li>
     * <li>403 - User not allowed to edit this task because of role(Authorization failure)</li>
     * <li>404 - Task not found for this owner</li>
     * <li>500 - Internal server error</li>
     * </ul>
     * </p>
     * 
     * @param request
     *            The incoming web request.
     * @param content
     *            The string with the task update Json.
     * @param id
     *            The id of the task to update.
     * @return A web response.
     */
    @Maintenance
    @Authorized(role = "tasks.write")
    @PUT
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response editTask(@Context HttpServletRequest request, String content, @PathParam("id") String id)
    {
        try
        {
            logger.info("Received request to edit task [" + id + "]");
            // Check if taskId is alphanumeric
            if (!TaskWebUtil.validateTaskId(id))
            {
                return TaskWebUtil.buildBadRequestResponse(MessageCodes.INVALID_TASKID_ERROR_CODE,
                        MessageCodes.INVALID_TASKID_ERROR_MESSAGE);
            }

            Profile profile = (Profile) request.getAttribute("profile");
            Task task = getDatabase().getTask(id);
            // Check task exists and is owned
            if (task == null)
            {
                logger.info("Could not find task [" + id + "]");
                return TaskWebUtil.buildNotFoundResponse(MessageCodes.TASK_NOT_FOUND_ERROR_CODE, MessageCodes.TASK_NOT_FOUND_ERROR_MESSAGE);
            }

            if (!profile.getId().equals(task.getOwner()))
            {
                logger.info("Could not find task [" + id + "] for the owner [" + task.getOwner() + "]");
                return TaskWebUtil.buildNotFoundResponse(MessageCodes.TASK_NOT_FOUND_ERROR_CODE, MessageCodes.TASK_NOT_FOUND_ERROR_MESSAGE);
            }
            // Authorize - TBD

            // Validate edit task is allowed
            String errMsg = TaskWebUtil.validateTaskEditable(task);
            if (errMsg != null)
            {
                return TaskWebUtil.buildConflictResponse(MessageCodes.CONFLICT_ERROR_CODE, errMsg);
            }

            // Validate request
            JsonObject updatedJson = JsonUtilities.fromJson(content, JsonObject.class);
            JsonObject errors = TaskWebUtil.validateEditTaskPayload(updatedJson);
            if (errors.size() > 0)
            {
                return Response.status(Status.BAD_REQUEST).entity(errors.toString()).build();
            }
            String secret = EnvironmentUtilities.getSecret(profile.getId(), EnvironmentUtilities.VAULT_SECRET);

            if (secret != null && !AesEncodingUtil.validateSecret(secret))
            {
                return Response.status(Status.BAD_REQUEST).entity("Invalid secret").build();
            }
            // Update task
            Task updatedTask = TaskWebUtil.updateTaskObject(task, updatedJson, secret, logger);
            getDatabase().updateTask(updatedTask);
            if (com.ibm.digital.mp.nestor.tasks.Status.COMPLETED.toString().equals(updatedTask.getStatus().toString()))
            {
                Executor executor = Executor.getInstance();
                Map<String, String> taskIds = executor.getAllTaskIdsByReferenceId(getDatabase(), updatedTask);
                for (Entry<String, String> entry : taskIds.entrySet())
                {
                    executor.asyncExecuteTask(entry.getKey(), entry.getValue(), com.ibm.digital.mp.nestor.tasks.Status.QUEUED.toString());
                }
            }
            return Response.ok(JsonUtilities.toExternalJson(updatedTask)).build();
        }
        catch (Exception ex)
        {
            logger.error("Error occurred in edit Task service", ex);
            return TaskWebUtil.buildServerErrorResponse(MessageCodes.SERVER_ERROR_CODE, MessageCodes.SERVER_ERROR_MESSAGE);
        }
    }

    /**
     * Execute task GET API.
     * 
     * @param request
     *            HttpServletRequest object
     * @param id
     *            of input task
     * @param revision
     *            of input task version
     * @return HttpServletResponse object
     */
    @Maintenance
    @Authorized(role = "tasks.execute")
    @POST
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response executeTask(@Context HttpServletRequest request, @PathParam("id") String id, @QueryParam("revision") String revision,
            @QueryParam("taskStatus") String taskStatus)
    {
        logger.info("Received request to execute task {} with _rev {}", id, revision);

        // Check if taskId is alphanumeric
        if (!TaskWebUtil.validateTaskId(id))
        {
            return TaskWebUtil.buildBadRequestResponse(MessageCodes.INVALID_TASKID_ERROR_CODE, MessageCodes.INVALID_TASKID_ERROR_MESSAGE);
        }
        Executor executor = Executor.getInstance();
        executor.asyncExecuteTask(id, revision, taskStatus);
        return Response.status(Status.ACCEPTED).build();

    }

    /**
     * GET a task.
     * 
     * @param request
     *            HttpServletRequest object
     * @param id
     *            of input task
     * @return HttpServletResponse object
     */
    @Authorized(role = "tasks.read")
    @GET
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTask(@Context HttpServletRequest request, @PathParam("id") String id)
    {

        try
        {
            logger.info("Received request to execute task [" + id + "]");
            // Check if taskId is alphanumeric
            if (!TaskWebUtil.validateTaskId(id))
            {
                return TaskWebUtil.buildBadRequestResponse(MessageCodes.INVALID_TASKID_ERROR_CODE,
                        MessageCodes.INVALID_TASKID_ERROR_MESSAGE);
            }

            Profile profile = (Profile) request.getAttribute(Constants.PROFILE);

            Task task = getDatabase().getTask(id);
            if (task == null)
            {
                logger.info("Could not find task [" + id + "]");
                return TaskWebUtil.buildNotFoundResponse(MessageCodes.TASK_NOT_FOUND_ERROR_CODE, MessageCodes.TASK_NOT_FOUND_ERROR_MESSAGE);
            }

            if (!profile.getId().equals(task.getOwner()))
            {
                logger.info("Could not find task [" + id + "] for the owner [" + task.getOwner() + "]");
                return TaskWebUtil.buildNotFoundResponse(MessageCodes.TASK_NOT_FOUND_ERROR_CODE, MessageCodes.TASK_NOT_FOUND_ERROR_MESSAGE);
            }

            return Response.ok(JsonUtilities.toExternalJson(task)).build();
        }
        catch (DbException ex)
        {
            logger.info("Could not find task [" + id + "]");
            return TaskWebUtil.buildNotFoundResponse(MessageCodes.TASK_NOT_FOUND_ERROR_CODE, MessageCodes.TASK_NOT_FOUND_ERROR_MESSAGE);
        }
        catch (Exception ex)
        {
            logger.info("Error occurred in get Task service", ex);
            return TaskWebUtil.buildServerErrorResponse(MessageCodes.SERVER_ERROR_CODE, MessageCodes.SERVER_ERROR_MESSAGE);
        }
    }

    /**
     * Get all tasks.
     * 
     * @param request
     *            request
     * @param queryJsonObject
     *            jsonObject
     * @return Response
     */
    @Authorized(role = "tasks.read")
    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTasks(@Context HttpServletRequest request, JsonObject queryJsonObject, @QueryParam("pageSize") int pageSize,
            @QueryParam("includePayload") boolean includePayload, @QueryParam("includeVault") boolean includeVault)
    {
        try
        {
            Profile profile = (Profile) request.getAttribute(Constants.PROFILE);
            logger.info("Received request to getTasks...");
            Map<String, String> queryParams = new HashMap<>();
            queryParams.put("owner", profile.getId());
            JsonObject selectorJsonObject = null;
            JsonObject newQueryJsonObject = new JsonObject();

            if (queryJsonObject != null)
            {
                if (queryJsonObject.has("selector"))
                {
                    selectorJsonObject = queryJsonObject.get("selector").getAsJsonObject();
                    if (selectorJsonObject.has("id"))
                    {
                        JsonElement idElem = selectorJsonObject.get("id");
                        if (idElem.isJsonObject())
                        {
                            JsonObject id = idElem.getAsJsonObject();
                            selectorJsonObject.add("_id", id);
                        }
                        else
                        {
                            String id = idElem.getAsString();
                            selectorJsonObject.addProperty("_id", id);
                        }
                        selectorJsonObject.remove("id");
                    }
                    if (selectorJsonObject.has("owner"))
                    {
                        return TaskWebUtil.buildBadRequestResponse(MessageCodes.MISSING_PAYLOAD_ERROR_CODE,
                                MessageCodes.MISSING_PAYLOAD_ERROR_MESSAGE);
                    }
                    selectorJsonObject.addProperty("owner", profile.getId());
                    newQueryJsonObject.add("selector", selectorJsonObject);
                }

                if (queryJsonObject.has("bookmark"))
                {
                    newQueryJsonObject.addProperty("bookmark", queryJsonObject.get("bookmark").getAsString());
                }
                if (queryJsonObject.has("sort"))
                {
                    newQueryJsonObject.add("sort", queryJsonObject.get("sort"));
                }
            }
            else
            {
                return TaskWebUtil.buildBadRequestResponse(MessageCodes.OWNER_NOT_ALLOWED_ERROR_CODE,
                        MessageCodes.OWNER_NOT_ALLOWED_ERROR_MESSAGE);
            }

            if (pageSize <= 0)
            {
                pageSize = 10;
            }

            logger.info("Received request to getTasks payload: {}", newQueryJsonObject.toString());
            AllTasks allTasks = getDatabase().getAllTasks(newQueryJsonObject, pageSize, includePayload, includeVault);

            JsonObject metadata = new JsonObject();
            metadata.addProperty("total", allTasks.getDocs().size());
            metadata.addProperty("count", allTasks.getDocs().size());
            metadata.addProperty("bookmark", allTasks.getBookmark());
            JsonObject responsePayload = new JsonObject();
            responsePayload.add("metadata", metadata);
            JsonArray taskList = JsonUtilities.convertToJson(allTasks.getDocs(), true, includePayload, includeVault);
            responsePayload.add("data", taskList);
            ResponseBuilder responseBuilder = Response.ok(responsePayload, MediaType.APPLICATION_JSON);
            Response response = responseBuilder.build();
            response.getHeaders().add("Access-Control-Allow-Headers", "*");
            return response;
        }
        catch (Exception ex)
        {
            logger.error(ex.getMessage(), ex);
            return TaskWebUtil.buildServerErrorResponse(MessageCodes.SERVER_ERROR_CODE, MessageCodes.SERVER_ERROR_MESSAGE);
        }
    }

    /**
     * Get count for task.
     * 
     * @param request
     *            request
     * @param referenceId
     *            string
     * @return response
     */
    @Authorized(role = "tasks.read")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTaskCountByReferenceId(@Context HttpServletRequest request, @QueryParam("referenceId") String referenceId)
    {
        try
        {
            Profile profile = (Profile) request.getAttribute(Constants.PROFILE);
            logger.info("Received request to getCountByIndex...");
            TaskWebUtil.validateGetTaskCountRequest(referenceId);
            String query = "referenceId:" + referenceId;
            TaskResult taskQueryResult = getCountByIndex("tasks/byReferenceId", query, profile.getId());
            JsonObject metadata = new JsonObject();
            metadata.addProperty("count", taskQueryResult.getTotalRows());
            metadata.addProperty("total", taskQueryResult.getTotalRows());
            metadata.addProperty("bookmark", taskQueryResult.getBookMark());
            JsonObject response = new JsonObject();
            response.add("metadata", metadata);
            if (taskQueryResult.getTaskJsonList() != null && !taskQueryResult.getTaskJsonList().isEmpty())
            {
                String tasks = new Gson().toJson(taskQueryResult.getTaskJsonList());
                JsonArray taskArray = new JsonParser().parse(tasks).getAsJsonArray();
                response.add("data", taskArray);
            }

            return Response.ok(response).build();

        }
        catch (AntillesWebException antillesWebException)
        {
            return TaskWebUtil.buildErrorResponse(antillesWebException);
        }
        catch (Exception ex)
        {
            logger.error(ex.getMessage(), ex);
            return TaskWebUtil.buildServerErrorResponse(MessageCodes.SERVER_ERROR_CODE, MessageCodes.SERVER_ERROR_MESSAGE);
        }
    }

    /**
     * Get count for task.
     * 
     * @param queryName
     *            string
     * @param query
     *            string
     * @param owner
     *            string
     * @return TaskQueryResult
     */
    public TaskResult getCountByIndex(String queryName, String query, String owner) throws DbException
    {

        TaskResult taskQueryResult = null;
        logger.info("In getCountByIndex...");
        String dbQuery = constructQuery(query, owner);
        taskQueryResult = getDatabase().getResultByIndexQuery(queryName, dbQuery, "[\"referenceId<string>\"]", false, 200, null);
        return taskQueryResult;

    }

    private String constructQuery(String query, String owner)
    {
        String dbQuery = "";
        String[] params = query.split("&");
        for (String param : params)
        {
            String[] fieldAndValue = param.split(":");
            dbQuery = dbQuery + fieldAndValue[0] + ":" + "\"" + fieldAndValue[1] + "\"" + " AND ";
        }
        dbQuery = dbQuery + "owner:" + "\"" + owner + "\"";

        return dbQuery;
    }

    protected Database getDatabase()
    {
        return DbFactory.getDatabase();
    }
}
