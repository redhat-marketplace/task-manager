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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.AntillesWebException;
import com.ibm.digital.mp.nestor.MessageCodes;
import com.ibm.digital.mp.nestor.antilles.util.DependentTaskUtil;
import com.ibm.digital.mp.nestor.antilles.util.TaskWebUtil;
import com.ibm.digital.mp.nestor.auth.Authorized;
import com.ibm.digital.mp.nestor.db.Database;
import com.ibm.digital.mp.nestor.db.DbException;
import com.ibm.digital.mp.nestor.db.DbFactory;
import com.ibm.digital.mp.nestor.db.TaskResult;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.util.Constants;
import com.ibm.digital.mp.nestor.util.JsonUtilities;

@Path("/v2")
public class TaskWebServiceV2
{
    private static final Logger logger = LogManager.getLogger(TaskWebServiceV2.class);

    private static final int DEFAULT_PAGE_SIZE = 10;

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
     * Get payload for given taskId.
     * 
     * @param request
     *            HttpServletRequest object
     * @return HttpServletResponce having task payload
     */
    @Authorized(role = "tasks.read")
    @PUT
    @Path("/tasks")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTasksByIndex(@Context HttpServletRequest request, JsonObject queryJsonObject)
    {
        try
        {
            logger.info("Received request to getTasks by indexId...");
            Profile profile = (Profile) request.getAttribute(Constants.PROFILE);
            Map<String, String> queryParams = new HashMap<>();
            queryParams.put("owner", profile.getId());

            // Required parameters searchIndexId & query
            String indexId = null;
            if (queryJsonObject.has("searchIndexId") && !queryJsonObject.get("searchIndexId").isJsonNull())
            {
                indexId = queryJsonObject.get("searchIndexId").getAsString();
            }

            String query = null;
            if (queryJsonObject.has("query") && !queryJsonObject.get("query").isJsonNull())
            {
                query = queryJsonObject.get("query").getAsString();
            }

            if (indexId == null || query == null)
            {
                return TaskWebUtil.buildBadRequestResponse(MessageCodes.MISSING_PAYLOAD_ERROR_CODE,
                        MessageCodes.MISSING_PAYLOAD_ERROR_MESSAGE);
            }

            // Optional parameters sort, includeDocs, pageSize & bookmark
            String sort = null;
            if (queryJsonObject.has("sort") && !queryJsonObject.get("sort").isJsonNull())
            {
                sort = queryJsonObject.get("sort").getAsString();
            }

            boolean includeDocs = false;
            if (queryJsonObject.has("includeDocs") && !queryJsonObject.get("includeDocs").isJsonNull())
            {
                includeDocs = queryJsonObject.get("includeDocs").getAsBoolean();
            }

            int limit = DEFAULT_PAGE_SIZE;
            if (queryJsonObject.has("pageSize") && !queryJsonObject.get("pageSize").isJsonNull())
            {
                limit = queryJsonObject.get("pageSize").getAsInt();
            }

            String bookmark = null;
            if (queryJsonObject.has("bookmark") && !queryJsonObject.get("bookmark").isJsonNull())
            {
                bookmark = queryJsonObject.get("bookmark").getAsString();
            }

            // add owner restriction
            query = query + " AND owner:" + "\"" + profile.getId() + "\"";

            logger.info("Input query: {}", query);
            TaskResult result = getDatabase().getResultByIndexQuery(indexId, query, sort, includeDocs, limit, bookmark);
            JsonObject response = new JsonObject();
            JsonArray taskList = JsonUtilities.convertToJson(result.getTaskJsonList(), true);
            response.add("data", taskList);
            JsonObject metadata = new JsonObject();
            metadata.addProperty("total", result.getTotalRows());
            metadata.addProperty("count", result.getTaskJsonList().size());
            metadata.addProperty("bookmark", result.getBookMark());
            metadata.addProperty("pageSize", limit);
            response.add("metadata", metadata);

            ResponseBuilder responseBuilder = Response.ok(response, MediaType.APPLICATION_JSON);
            return responseBuilder.build();
        }
        catch (Exception ex)
        {
            logger.error(ex.getMessage(), ex);
            return TaskWebUtil.buildServerErrorResponse(MessageCodes.SERVER_ERROR_CODE, MessageCodes.SERVER_ERROR_MESSAGE);
        }
    }

    /**
     * Get dependent task list for a specific task.
     * 
     * @param request.
     * @param taskId
     *            - requested Task id.
     * @return JSON data with dependent task list.
     */
    @Authorized(role = "tasks.read")
    @GET
    @Path("/tasks/{id}/dependencies")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDependentTasks(@Context HttpServletRequest request, @PathParam("id") String taskId)
    {
        try
        {
            logger.info("Received request to getDependentTasks by taksId...");

            JsonObject dbResponse = getDatabase().getDependentTasks(taskId);

            if (dbResponse == null)
            {
                return TaskWebUtil.buildServerErrorResponse(MessageCodes.INVALID_TASKID_ERROR_CODE,
                        MessageCodes.INVALID_TASKID_ERROR_MESSAGE);
            }

            ResponseBuilder responseBuilder = Response.ok(DependentTaskUtil.buildTaskHierarchy(dbResponse), MediaType.APPLICATION_JSON);

            return responseBuilder.build();
        }
        catch (Exception ex)
        {
            logger.error(ex.getMessage(), ex);
            return TaskWebUtil.buildServerErrorResponse(MessageCodes.SERVER_ERROR_CODE, MessageCodes.SERVER_ERROR_MESSAGE);
        }
    }

    
    /**
     * Get task count by query.
     * @param request HttpServletRequest
     * @param queryJson JsonObject
     * @return Response
     */
    @Authorized(role = "tasks.read")
    @PUT
    @Path("/tasks/count")
    @Produces(MediaType.APPLICATION_JSON)    
    public Response getTasksCount(@Context HttpServletRequest request, JsonObject queryJson)
    {
        try
        {
            logger.info("Received request to getTasksCount with payload {}", queryJson);
            Profile profile = (Profile) request.getAttribute(Constants.PROFILE);

            TaskWebUtil.validateGetTasksCountRequest(queryJson);
            List<Bson> filters = TaskWebUtil.prepareBsonQuery(queryJson, profile);
            logger.info("Input queryParams: {}", filters);
            long totalCount = getDatabase().getTaskCountByQuery(filters);
            JsonObject result = new JsonObject();
            result.addProperty("count", totalCount);
            return Response.ok(result.toString()).build();

        }

        catch (AntillesWebException antillesWebException)
        {
            logger.error(antillesWebException.getMessage(), antillesWebException);
            return TaskWebUtil.buildErrorResponse(antillesWebException);
        }
        catch (Exception ex)
        {
            logger.error(ex.getMessage(), ex);
            return TaskWebUtil.buildServerErrorResponse(MessageCodes.SERVER_ERROR_CODE, MessageCodes.SERVER_ERROR_MESSAGE);
        }
    }

    protected Database getDatabase()
    {
        return DbFactory.getDatabase();
    }
}
