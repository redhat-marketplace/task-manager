package com.ibm.digital.mp.nestor.antilles.tasks.recipes.samples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.HttpHeaders;

import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.SystemException;
import com.ibm.digital.mp.nestor.tasks.ExecutionContext;
import com.ibm.digital.mp.nestor.tasks.ReservedTaskSteps;
import com.ibm.digital.mp.nestor.tasks.Result;
import com.ibm.digital.mp.nestor.tasks.Result.Status;
import com.ibm.digital.mp.nestor.tasks.TaskExecutionException;
import com.ibm.digital.mp.nestor.tasks.recipes.AbstractRecipe;
import com.ibm.digital.mp.nestor.tasks.recipes.util.RecipeUtilities;
import com.ibm.digital.mp.nestor.tasks.recipes.util.Response;
import com.ibm.digital.mp.nestor.util.JsonUtilities;

public class ActiveDummyRecipe extends AbstractRecipe
{

    private static final Logger logger = LogManager.getLogger(ActiveDummyRecipe.class);
    RecipeUtilities recipeUtil = new RecipeUtilities();

    @Override
    public Result execute(ExecutionContext executionContext) throws TaskExecutionException
    {
        Result result = null;

        if (ReservedTaskSteps.START_STEP.equals(executionContext.getStepId()))
        {
            JsonObject vault = new JsonObject();
            return new Result("SUBMIT_TASKS", Status.RUNNING, vault);
        }
        else if ("SUBMIT_TASKS".equals(executionContext.getStepId()))
        {
            JsonObject vault = new JsonObject();
            if (submitTasks(vault, executionContext.getConfiguration()))
            {
                long currentTime = System.currentTimeMillis();
                long scheduledTime = currentTime + (5 * 60 * 1000);
                return new Result("CHECK_TASKS_FOR_COMPLETION", Status.RUNNING, vault, scheduledTime);
            }
            throw new TaskExecutionException("Could not submit tasks");
        }
        else if ("CHECK_TASKS_FOR_COMPLETION".equals(executionContext.getStepId()))
        {
            logger.info("checking tasks for completion: " + executionContext.getPayload().get("id"));

            if (checkTaskForCompletion(executionContext.getVault(), executionContext.getConfiguration()))
            {
                long currentTime = System.currentTimeMillis();
                long scheduledTime = currentTime + (60 * 60 * 1000);
                return new Result(ReservedTaskSteps.END_STEP, Result.Status.RUNNING, executionContext.getVault(), scheduledTime);
            }

        }

        return result;
    }

    private boolean checkTaskForCompletion(JsonObject vault, JsonObject configuration)
    {
        boolean taskCompleted = false;
        String referenceId = vault.get("referenceId").getAsString();
        JsonObject jsonObject = new JsonObject();
        JsonObject selectorJsonObject = new JsonObject();
        selectorJsonObject.addProperty("referenceId", referenceId);
        jsonObject.add("selector", selectorJsonObject);
        String subject = configuration.get("subject").getAsString();
        String secret = configuration.get("secret").getAsString();
        String jwtToken = null;
        try
        {
            Algorithm algorithm = Algorithm.HMAC256(secret);
            jwtToken = JWT.create().withSubject(subject).sign(algorithm);
        }
        catch (IllegalArgumentException ex)
        {
            logger.error(ex.getMessage(), ex);
        }

        Map<String, String> headers = new HashMap<>();
        headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + jwtToken);
        headers.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        String antillesBaseUrl = configuration.get("antillesBaseUrl").getAsString();
        String antillesUrl = antillesBaseUrl + "/v1/tasks";
        Response response;
        JsonObject allTasksJsonObject = null;
        try
        {
            response = recipeUtil.sendHttpRequest("PUT", antillesUrl, headers, jsonObject.toString(), "antilles");
            allTasksJsonObject = JsonUtilities.fromJson(response.getEntity(), JsonObject.class);
        }
        catch (SystemException ex)
        {
            logger.error(ex.getMessage(), ex);
            return taskCompleted;
        }
        JsonArray taskArray = allTasksJsonObject.get("data").getAsJsonArray();
        int numberOfTasks = taskArray.size();
        int successTasks = 0;
        for (int count = 0; count < numberOfTasks; count++)
        {
            JsonObject task = taskArray.get(count).getAsJsonObject();
            if (!task.get("status").toString().equals("QUEUED") && !task.get("status").toString().equals("FAILED")
                    && !task.get("status").toString().equals("NEEDS_ATTENTION"))
            {
                successTasks++;
            }
        }
        if (numberOfTasks == successTasks)
        {
            taskCompleted = true;
        }
        return taskCompleted;

    }

    private boolean submitTasks(JsonObject vault, JsonObject configuration)
    {
        boolean allTasksSubmitted = false;
        logger.info("Starting active Task: ");
        String subject = configuration.get("subject").getAsString();
        String secret = configuration.get("secret").getAsString();
        String jwtToken = null;
        try
        {
            Algorithm algorithm = Algorithm.HMAC256(secret);
            jwtToken = JWT.create().withSubject(subject).sign(algorithm);
        }
        catch (IllegalArgumentException ex)
        {
            logger.error(ex.getMessage(), ex);
        }

        Map<String, String> headers = new HashMap<>();
        headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + jwtToken);
        headers.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        String referenceId = UUID.randomUUID().toString();
        String antillesBaseUrl = configuration.get("antillesBaseUrl").getAsString();
        String antillesUrl = antillesBaseUrl + "/v1/tasks";
        vault.addProperty("referenceId", referenceId);
        List<String> typeList = new ArrayList<>();
        typeList.add("dummy");
        typeList.add("scheduledDummy");
        typeList.add("failingDummy");
        typeList.add("pollingDummy");
        typeList.add("dummy");
        Integer count = 0;
        for (String type : typeList)
        {
            count++;
            try
            {
                Response response = recipeUtil.sendHttpRequest("POST", antillesUrl, headers, createPayload(type, referenceId), "antilles");
                JsonObject task = JsonUtilities.fromJson(response.getEntity(), JsonObject.class);

                vault.addProperty(count.toString(), task.get("id").getAsString());
                allTasksSubmitted = true;
            }
            catch (SystemException ex)
            {
                logger.error(ex.getMessage(), ex);
            }
        }
        return allTasksSubmitted;
    }

    private String createPayload(String type, String referenceId)
    {
        JsonObject payload = new JsonObject();
        payload.addProperty("type", type);
        payload.addProperty("referenceId", referenceId);
        JsonObject taskPayload = new JsonObject();
        taskPayload.addProperty("id", 1);
        payload.add("payload", taskPayload);

        return payload.toString();

    }

}
