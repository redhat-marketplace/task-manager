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

package com.ibm.digital.mp.nestor.antilles.webhooks;

import java.text.ParseException;
import java.util.Date;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.antilles.tasks.web.filters.Maintenance;
import com.ibm.digital.mp.nestor.antilles.util.TaskWebUtil;
import com.ibm.digital.mp.nestor.api.NestorApi;
import com.ibm.digital.mp.nestor.api.NestorException;
import com.ibm.digital.mp.nestor.api.impl.NestorApiImpl;
import com.ibm.digital.mp.nestor.auth.Authorized;
import com.ibm.digital.mp.nestor.db.Database;
import com.ibm.digital.mp.nestor.db.DbException;
import com.ibm.digital.mp.nestor.db.DbFactory;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.tasks.WebServiceHook;
import com.ibm.digital.mp.nestor.util.JsonUtilities;

@Path("/v1/webhooks")
public class WebhookService
{
    private static final Logger logger = LogManager.getLogger(WebhookService.class);


    /**
     * Execute webhook task..
     * 
     * @param request
     *            HttpServletRequest object
     * @return HttpServletResponse object
     * @throws WebhookAuthenticationException throw error for webhook authentication.
     * @throws NestorException nestor exception.
     * @throws ParseException parsexception.
     * @throws WebhookTransformationException transformesxception.
     */
    @Authorized
    @Maintenance
    @POST
    @Path("{profileId}/{webhookId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response executeWebhookTask(@Context HttpServletRequest request, String content, @PathParam("webhookId") String webhookId,
            @PathParam("profileId") String profileId)
            throws WebhookAuthenticationException, NestorException, ParseException, WebhookTransformationException
    {
        logger.info("Received request to execute task for profile {} with webhook {}", profileId, webhookId);

        // Do the validation against defined profile

        // 1. Need to load complete profile first with authentication.

        Profile profile = null;
        // Loading profile based on the Subject
        try
        {
            if (profileId == null || profileId.isEmpty())
            {
                throw new WebhookAuthenticationException("Provided profileId is not correct one.", "ERROR-" + HttpStatus.SC_UNAUTHORIZED);
            }

            profile = getDatabase().getProfile(profileId);

            
            if (profile == null)
            {
                throw new WebhookAuthenticationException("Provided profileId is not correct one.", "ERROR-" + HttpStatus.SC_UNAUTHORIZED);
            }
            
            if (profile.hasWebhook(webhookId))
            {
                WebServiceHook webhookServiceHook = profile.getWebhook(webhookId);

                // Do Transformation now.
                TransformedTask transformedTask = new WebhookTransformation().transformTask(webhookServiceHook.getTransformer(), content);

                if (transformedTask != null)
                {

                    NestorApi nestorapi = new NestorApiImpl(profile);

                    Date prefExecutionDate = transformedTask.getPreferredExecutionDate() != null
                            & !"".equals(transformedTask.getPreferredExecutionDate())
                                    ? new Date(transformedTask.getPreferredExecutionDate()) : new Date();
                    
                    String taskId = nestorapi.createTask(transformedTask.getType(), transformedTask.getReferenceId(),
                            Boolean.valueOf(transformedTask.getSequenceByReferenceId()),transformedTask.getParentReferenceId(),
                            Boolean.valueOf(transformedTask.getSequenceByParentReferenceId()),
                            prefExecutionDate,
                            JsonUtilities.fromJson(transformedTask.getPayload(), JsonObject.class));

                    logger.info("Created task [" + taskId + "].");
                }
                else
                {
                    throw new WebhookTransformationException("Not able to transform provided input.");
                }

                JsonObject responseStringJson = new JsonObject();
                
                return Response.ok(JsonUtilities.toExternalJson(responseStringJson)).build();
            }
            else
            {
                logger.error("Wrong Webhook passed.");
                throw new WebhookAuthenticationException("Provided webhook is not correct one.", 
                        "ERROR-" + HttpStatus.SC_UNAUTHORIZED);
            }
        }
        catch (DbException ex)
        {
            logger.error("Error: ", ex);
            return TaskWebUtil.buildErrorResponse(ex.getMessage(), ex.getMessage(), null);
        } 
        catch (WebhookAuthenticationException ex)
        {
            logger.error("Authentication failed.", ex);
            return TaskWebUtil.buildUnauthorizedResponse(ex.getErrorCode(), ex.getMessage());
        }
    }

    /**
     * getDatabase method.
     * @return database object.
     */
    protected Database getDatabase()
    {
        return DbFactory.getDatabase();
    }
    
}
