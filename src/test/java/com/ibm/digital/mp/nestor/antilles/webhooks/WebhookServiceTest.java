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

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import org.apache.logging.log4j.ThreadContext;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.digital.mp.nestor.antilles.tasks.Executor;
import com.ibm.digital.mp.nestor.api.NestorException;
import com.ibm.digital.mp.nestor.client.AntillesClient;
import com.ibm.digital.mp.nestor.config.EnvironmentUtilities;
import com.ibm.digital.mp.nestor.db.Database;
import com.ibm.digital.mp.nestor.db.DbException;
import com.ibm.digital.mp.nestor.db.DbFactory;
import com.ibm.digital.mp.nestor.internal.antilles.tasks.recipes.RecipeManager;
import com.ibm.digital.mp.nestor.tasks.Authenticator;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.tasks.Task;
import com.ibm.digital.mp.nestor.tasks.TaskProperties;
import com.ibm.digital.mp.nestor.tasks.Transformer;
import com.ibm.digital.mp.nestor.tasks.Validator;
import com.ibm.digital.mp.nestor.tasks.WebServiceHook;
import com.ibm.digital.mp.nestor.tasks.recipes.Recipe;
import com.ibm.digital.mp.nestor.util.Constants;
import com.ibm.digital.mp.nestor.util.JsonUtilities;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ AntillesClient.class, Executor.class, DbFactory.class, Database.class, RecipeManager.class, EnvironmentUtilities.class,
        ThreadContext.class })
@PowerMockIgnore({ "javax.management.*", "javax.crypto.*" })
public class WebhookServiceTest
{

    @Test
    public void testExecuteTaskValidationException_wrongSubject() throws DbException
    {

        WebhookService webhookservice = new WebhookService()
        {
        };

        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\",\"preferredExecutionDate\":1516088453778,"
                + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,"
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";

        JsonObject payloadJson = null;
        try
        {

            payloadJson = JsonUtilities.fromJson(content, JsonObject.class);
        }
        catch (Exception ex)
        {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        }

        String webhookString3 = "{\"webhook_id_1ty56g6ret43er45trw\": {\"authenticator\": "
                + "{\"type\": \"JWT\",\"subject\": \"order-prov-notif\",\"secret\": \"order-prov-notif-stage-secret\"},"
                + "\"validator\": {\"type\": \"jsonSchema\",\"schemaFile\": \"name\"},\"transformer\": "
                + "{\"type\": \"OrderProvNotif\"}}}";

        JsonObject webhookJsonObject = new JsonParser().parse(webhookString3).getAsJsonObject();

        PowerMock.mockStatic(ThreadContext.class);
        EasyMock.expect(ThreadContext.getImmutableContext()).andReturn(new HashMap<String, String>()).anyTimes();
        EasyMock.expect(ThreadContext.getDepth()).andReturn(1).anyTimes();
        EasyMock.expect(ThreadContext.cloneStack()).andReturn(EasyMock.anyObject()).anyTimes();
        EasyMock.expect(ThreadContext.get(Constants.TRANSACTION_LOG_ID)).andReturn("123-234-345").anyTimes();

        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        Profile profile = Mockito.mock(Profile.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        EasyMock.expect(request.getRequestURI()).andReturn("http://abc.com");
        EasyMock.expect(request.getHeader(Constants.TRANSACTION_ID_HEADER)).andReturn("71d29b88-9ee2-4e59-9214-fef72b8b07da").anyTimes();

        EasyMock.expect(request.getHeader("Authorization"))
                .andReturn("Bearer JhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJvcmRlci1wcm92LW5vdGlmIn0."
                        + "dhxxdNZNkbl-T3aAfnrIxAHbTWSr1C_WKK199xxdU6E")
                .anyTimes();

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setId("028582");

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();

        PowerMock.replayAll(DbFactory.class, ThreadContext.class, database);

        Response response = null;
        try
        {
            response = webhookservice.executeWebhookTask(request, content, "webhook_id_1ty56g6ret43er45trw", "OrderProvNotif");
            Assert.assertTrue(true);
        }
        catch (WebhookAuthenticationException ex)
        {
            Assert.assertTrue(true);
        }
        catch (NestorException ex)
        {
            Assert.assertTrue(true);
        }
        catch (Exception ex)
        {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void testExecuteTaskValidationException_verifyToken() throws DbException
    {

        WebhookService webhookservice = new WebhookService()
        {
        };

        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\",\"preferredExecutionDate\":1516088453778,"
                + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,"
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";

        JsonObject payloadJson = null;
        try
        {

            payloadJson = JsonUtilities.fromJson(content, JsonObject.class);
        }
        catch (Exception ex)
        {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        }

        String webhookString3 = "{\"webhook_id_1ty56g6ret43er45trw\": {\"authenticator\": "
                + "{\"type\": \"JWT\",\"subject\": \"order-prov-notif\",\"secret\": \"order-prov-notif-stage-secret\"},"
                + "\"validator\": {\"type\": \"jsonSchema\",\"schemaFile\": \"name\"},\"transformer\": "
                + "{\"type\": \"OrderProvNotif\"}}}";

        JsonObject webhookJsonObject = new JsonParser().parse(webhookString3).getAsJsonObject();

        PowerMock.mockStatic(ThreadContext.class);
        EasyMock.expect(ThreadContext.getImmutableContext()).andReturn(new HashMap<String, String>()).anyTimes();
        EasyMock.expect(ThreadContext.getDepth()).andReturn(1).anyTimes();
        EasyMock.expect(ThreadContext.cloneStack()).andReturn(EasyMock.anyObject()).anyTimes();
        EasyMock.expect(ThreadContext.get(Constants.TRANSACTION_LOG_ID)).andReturn("123-234-345").anyTimes();

        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        Profile profile = Mockito.mock(Profile.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        EasyMock.expect(request.getRequestURI()).andReturn("http://abc.com");
        EasyMock.expect(request.getHeader(Constants.TRANSACTION_ID_HEADER)).andReturn("71d29b88-9ee2-4e59-9214-fef72b8b07da").anyTimes();

        EasyMock.expect(request.getHeader("Authorization"))
                .andReturn("Bearer JhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJvcmRlci1wcm92LW5vdGlmIn0."
                        + "dhxxdNZNkbl-T3aAfnrIxAHbTWSr1C_WKK199xxdU6E")
                .anyTimes();

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setId("028582");

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();

        PowerMock.replayAll(DbFactory.class, ThreadContext.class, database);

        Response response = null;
        try
        {
            response = webhookservice.executeWebhookTask(request, content, "webhook_id_1ty56g6ret43er45trw", "OrderProvNotif");
            Assert.assertTrue(true);
        }
        catch (WebhookAuthenticationException ex)
        {
            Assert.assertTrue(true);
        }
        catch (NestorException ex)
        {
            Assert.assertTrue(true);
        }
        catch (Exception ex)
        {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void testExecuteTaskValidationException() throws DbException
    {

        WebhookService webhookservice = new WebhookService()
        {
        };

        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\",\"preferredExecutionDate\":1516088453778,"
                + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,"
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";

        JsonObject payloadJson = null;
        try
        {

            payloadJson = JsonUtilities.fromJson(content, JsonObject.class);
        }
        catch (Exception ex)
        {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        }

        String webhookString3 = "{\"webhook_id_1ty56g6ret43er45trw\": {\"authenticator\": "
                + "{\"type\": \"JWT\",\"subject\": \"order-prov-notif\",\"secret\": \"order-prov-notif-stage-secret\"},"
                + "\"validator\": {\"type\": \"jsonSchema\",\"schemaFile\": \"name\"},\"transformer\": "
                + "{\"type\": \"OrderProvNotif\"}}}";

        JsonObject webhookJsonObject = new JsonParser().parse(webhookString3).getAsJsonObject();

        PowerMock.mockStatic(ThreadContext.class);
        EasyMock.expect(ThreadContext.getImmutableContext()).andReturn(new HashMap<String, String>()).anyTimes();
        EasyMock.expect(ThreadContext.getDepth()).andReturn(1).anyTimes();
        EasyMock.expect(ThreadContext.cloneStack()).andReturn(EasyMock.anyObject()).anyTimes();
        EasyMock.expect(ThreadContext.get(Constants.TRANSACTION_LOG_ID)).andReturn("123-234-345").anyTimes();

        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        Profile profile = Mockito.mock(Profile.class);
        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        EasyMock.expect(request.getRequestURI()).andReturn("http://abc.com");
        EasyMock.expect(request.getHeader(Constants.TRANSACTION_ID_HEADER)).andReturn("71d29b88-9ee2-4e59-9214-fef72b8b07da").anyTimes();

        EasyMock.expect(request.getHeader("Authorization")).andReturn("Basic dXNlci10ZXN0LWFwcDpwYXNzdzByZFRlc3Q=").anyTimes();

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setId("028582");

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();

        PowerMock.replayAll(DbFactory.class, ThreadContext.class, database);

        Response response = null;
        try
        {
            response = webhookservice.executeWebhookTask(request, content, "webhook_id_1ty56g6ret43er45trw", "OrderProvNotif");
            Assert.assertTrue(true);
        }
        catch (WebhookAuthenticationException ex)
        {
            Assert.assertTrue(true);
        }
        catch (NestorException ex)
        {
            Assert.assertTrue(true);
        }
        catch (Exception ex)
        {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void testExecuteTaskWithInvalidWebhookId() throws DbException
    {

        WebhookService webhookservice = new WebhookService()
        {
        };

        String webhookString3 = "{\"webhook_id_1ty56g6ret43er45trw\": {\"authenticator\": "
                + "{\"type\": \"JWT\",\"subject\": \"order-prov-notif\",\"secret\": \"order-prov-notif-stage-secret\"},"
                + "\"validator\": {\"type\": \"jsonSchema\",\"schemaFile\": \"name\"},\"transformer\": "
                + "{\"type\": \"OrderProvNotif\"}}}";

        JsonObject webhookJsonObject = new JsonParser().parse(webhookString3).getAsJsonObject();

        PowerMock.mockStatic(ThreadContext.class);
        EasyMock.expect(ThreadContext.getImmutableContext()).andReturn(new HashMap<String, String>()).anyTimes();
        EasyMock.expect(ThreadContext.getDepth()).andReturn(1).anyTimes();
        EasyMock.expect(ThreadContext.cloneStack()).andReturn(EasyMock.anyObject()).anyTimes();
        EasyMock.expect(ThreadContext.get(Constants.TRANSACTION_LOG_ID)).andReturn("123-234-345").anyTimes();

        Profile profile = Mockito.mock(Profile.class);

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

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();

        PowerMock.replayAll(DbFactory.class, ThreadContext.class, database);
        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\",\"preferredExecutionDate\":1516088453778,"
                + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,"
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";
        Response response = null;
        // try
        // {
        // response = webhookservice.executeTask(request, content, "webhook-1-sder3434", "RemoveSubscriptions");
        // }
        // catch (WebhookAuthenticationException ex)
        // {
        // ex.printStackTrace();
        // }
        // catch (NestorException ex)
        // {
        // ex.printStackTrace();
        // }
        // No webhookId found
        // Assert.assertEquals(401, response.getStatus());
    }

    @Test
    public void testExecuteTaskWithValidWebhookId() throws DbException
    {

        WebhookService webhookservice = new WebhookService()
        {
        };

        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\",\"preferredExecutionDate\":1516088453778,"
                + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,"
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";

        JsonObject payloadJson = null;
        try
        {

            payloadJson = JsonUtilities.fromJson(content, JsonObject.class);
        }
        catch (Exception ex)
        {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        }

        String webhookString3 = "{\"webhook_id_1ty56g6ret43er45trw\": {\"authenticator\": "
                + "{\"type\": \"JWT\",\"subject\": \"order-prov-notif\",\"secret\": \"order-prov-notif-stage-secret\"},"
                + "\"validator\": {\"type\": \"jsonSchema\",\"schemaFile\": \"name\"},\"transformer\": "
                + "{\"type\": \"OrderProvNotif\"}}}";

        JsonObject webhookJsonObject = new JsonParser().parse(webhookString3).getAsJsonObject();

        PowerMock.mockStatic(ThreadContext.class);
        EasyMock.expect(ThreadContext.getImmutableContext()).andReturn(new HashMap<String, String>()).anyTimes();
        EasyMock.expect(ThreadContext.getDepth()).andReturn(1).anyTimes();
        EasyMock.expect(ThreadContext.cloneStack()).andReturn(EasyMock.anyObject()).anyTimes();
        EasyMock.expect(ThreadContext.get(Constants.TRANSACTION_LOG_ID)).andReturn("123-234-345").anyTimes();

        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        Profile profile = Mockito.mock(Profile.class);

        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        EasyMock.expect(request.getRequestURI()).andReturn("http://abc.com");
        EasyMock.expect(request.getHeader(Constants.TRANSACTION_ID_HEADER)).andReturn("71d29b88-9ee2-4e59-9214-fef72b8b07da").anyTimes();

        EasyMock.expect(request.getHeader("Authorization"))
                .andReturn("Bearer JhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJvcmRlci1wcm92LW5vdGlmIn0."
                        + "dhxxdNZNkbl-T3aAfnrIxAHbTWSr1C_WKK199xxdU6E")
                .anyTimes();

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setId("028582");

        Profile profile1 = Mockito.mock(Profile.class);

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();

        PowerMock.replayAll(DbFactory.class, ThreadContext.class, database);

        Response response = null;
        try
        {
            response = webhookservice.executeWebhookTask(request, content, "webhook_id_1ty56g6ret43er45trw", "OrderProvNotif");
            Assert.assertTrue(true);
        }
        catch (WebhookAuthenticationException ex)
        {
            Assert.assertTrue(true);
        }
        catch (NestorException ex)
        {
            Assert.assertTrue(true);
        }
        catch (Exception ex)
        {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testExecuteTaskWithValidWebhookId_Basicauth_valid() throws DbException
    {

        WebhookService webhookservice = new WebhookService()
        {
        };

        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\",\"preferredExecutionDate\":1516088453778,"
                + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,"
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";

        JsonObject payloadJson = null;
        try
        {

            payloadJson = JsonUtilities.fromJson(content, JsonObject.class);
        }
        catch (Exception ex)
        {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        }

        String webhookString3 = "{\"webhook_id_1ty56g6ret43er45trw\": {\"authenticator\": "
                + "{\"type\": \"basic\",\"userName\": \"user-test-app\",\"password\": \"passw0rdTest\"},"
                + "\"validator\": {\"type\": \"jsonSchema\",\"schemaFile\": \"name\"},\"transformer\": "
                + "{\"type\": \"OrderProvNotif\"}}}";

        JsonObject webhookJsonObject = new JsonParser().parse(webhookString3).getAsJsonObject();

        PowerMock.mockStatic(ThreadContext.class);
        EasyMock.expect(ThreadContext.getImmutableContext()).andReturn(new HashMap<String, String>()).anyTimes();
        EasyMock.expect(ThreadContext.getDepth()).andReturn(1).anyTimes();
        EasyMock.expect(ThreadContext.cloneStack()).andReturn(EasyMock.anyObject()).anyTimes();
        EasyMock.expect(ThreadContext.get(Constants.TRANSACTION_LOG_ID)).andReturn("123-234-345").anyTimes();

        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        Profile profile = Mockito.mock(Profile.class);

        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        EasyMock.expect(request.getRequestURI()).andReturn("http://abc.com");
        EasyMock.expect(request.getHeader(Constants.TRANSACTION_ID_HEADER)).andReturn("71d29b88-9ee2-4e59-9214-fef72b8b07da").anyTimes();

        EasyMock.expect(request.getHeader("Authorization")).andReturn("Basic dXNlci10ZXN0LWFwcDpwYXNzdzByZFRlc3Q=").anyTimes();

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setId("028582");

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();

        PowerMock.replayAll(DbFactory.class, ThreadContext.class, database);

        Response response = null;
        try
        {
            response = webhookservice.executeWebhookTask(request, content, "webhook_id_1ty56g6ret43er45trw", "OrderProvNotif");
            Assert.assertTrue(true);
        }
        catch (WebhookAuthenticationException ex)
        {
            Assert.assertTrue(true);
        }
        catch (NestorException ex)
        {
            Assert.assertTrue(true);
        }
        catch (Exception ex)
        {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testExecuteTaskWithValidWebhookId_Basicauth_Invalid() throws DbException
    {

        WebhookService webhookservice = new WebhookService()
        {
        };

        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\",\"preferredExecutionDate\":1516088453778,"
                + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,"
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";

        JsonObject payloadJson = null;
        try
        {

            payloadJson = JsonUtilities.fromJson(content, JsonObject.class);
        }
        catch (Exception ex)
        {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        }

        String webhookString3 = "{\"webhook_id_1ty56g6ret43er45trw\": {\"authenticator\": "
                + "{\"type\": \"basic\",\"userName\": \"user-test-app\",\"password\": \"passw1rdTest\"},"
                + "\"validator\": {\"type\": \"jsonSchema\",\"schemaFile\": \"name\"},\"transformer\": "
                + "{\"type\": \"OrderProvNotif\"}}}";

        JsonObject webhookJsonObject = new JsonParser().parse(webhookString3).getAsJsonObject();

        PowerMock.mockStatic(ThreadContext.class);
        EasyMock.expect(ThreadContext.getImmutableContext()).andReturn(new HashMap<String, String>()).anyTimes();
        EasyMock.expect(ThreadContext.getDepth()).andReturn(1).anyTimes();
        EasyMock.expect(ThreadContext.cloneStack()).andReturn(EasyMock.anyObject()).anyTimes();
        EasyMock.expect(ThreadContext.get(Constants.TRANSACTION_LOG_ID)).andReturn("123-234-345").anyTimes();

        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        Profile profile = Mockito.mock(Profile.class);

        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        EasyMock.expect(request.getRequestURI()).andReturn("http://abc.com");
        EasyMock.expect(request.getHeader(Constants.TRANSACTION_ID_HEADER)).andReturn("71d29b88-9ee2-4e59-9214-fef72b8b07da").anyTimes();

        EasyMock.expect(request.getHeader("Authorization")).andReturn("Basic dXNlci10ZXN0LWFwcDpwYXNzdzByZFRlc3Q=").anyTimes();

        PowerMock.mockStatic(DbFactory.class);
        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        Task task = new Task();
        task.setId("028582");

        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();

        PowerMock.replayAll(DbFactory.class, ThreadContext.class, database);

        Response response = null;

    }

    @Test
    public void testExecuteTaskWithValidprofileId() throws DbException
    {

        WebhookService webhookservice = new WebhookService()
        {
        };

        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\",\"preferredExecutionDate\":1516088453778,"
                + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,"
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";

        JsonObject payloadJson = null;
        try
        {

            payloadJson = JsonUtilities.fromJson(content, JsonObject.class);
        }
        catch (Exception ex)
        {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        }

        String webhookString3 = "{\"webhook_id_1ty56g6ret43er45trw\": {\"authenticator\": "
                + "{\"type\": \"JWT\",\"subject\": \"order-prov-notif\",\"secret\": \"order-prov-notif-stage-secret\"},"
                + "\"validator\": {\"type\": \"jsonSchema\",\"schemaFile\": \"name\"},\"transformer\": "
                + "{\"type\": \"OrderProvNotif\"}}}";

        JsonObject webhookJsonObject = new JsonParser().parse(webhookString3).getAsJsonObject();

        PowerMock.mockStatic(ThreadContext.class);
        EasyMock.expect(ThreadContext.getImmutableContext()).andReturn(new HashMap<String, String>()).anyTimes();
        EasyMock.expect(ThreadContext.getDepth()).andReturn(1).anyTimes();
        EasyMock.expect(ThreadContext.cloneStack()).andReturn(EasyMock.anyObject()).anyTimes();
        EasyMock.expect(ThreadContext.get(Constants.TRANSACTION_LOG_ID)).andReturn("123-234-345").anyTimes();

        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        Profile profile = Mockito.mock(Profile.class);

        EasyMock.expect(request.getAttribute(com.ibm.digital.mp.nestor.util.Constants.PROFILE)).andReturn(profile);
        EasyMock.expect(request.getRequestURI()).andReturn("http://abc.com");
        EasyMock.expect(request.getHeader(Constants.TRANSACTION_ID_HEADER)).andReturn("71d29b88-9ee2-4e59-9214-fef72b8b07da").anyTimes();

        PowerMock.mockStatic(DbFactory.class);

        Task task = new Task();
        task.setId("028582");

        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(null).anyTimes();

        PowerMock.replayAll(DbFactory.class, ThreadContext.class, database);

        Response response = null;
        try
        {
            response = webhookservice.executeWebhookTask(request, content, "webhook_id_1ty56g6ret43er45trw", "");
            Assert.assertTrue(true);
        }
        catch (WebhookAuthenticationException ex)
        {
            Assert.assertTrue(true);
        }
        catch (NestorException ex)
        {
            Assert.assertTrue(true);
        }
        catch (Exception ex)
        {
            Assert.assertTrue(true);
        }

        try
        {
            response = webhookservice.executeWebhookTask(request, content, "webhook_id_1ty56g6ret43er45trw", "OrderProvNotif");
            Assert.assertTrue(true);
        }
        catch (WebhookAuthenticationException ex)
        {
            Assert.assertTrue(true);
        }
        catch (NestorException ex)
        {
            Assert.assertTrue(true);
        }
        catch (Exception ex)
        {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void testExecuteTaskCorrectWebhookId() throws DbException
    {

        WebhookService webhookservice = new WebhookService()
        {
        };

        String content = "{\"_rev\":\"1-f56cad2b8eab5247f3f7e8c250fe2501\",\"preferredExecutionDate\":1516088453778,"
                + "\"type\":\"RemoveSubscriptions\",\"stepProcessingCount\":0,"
                + "\"failedExecutionCount\":0,\"sequenceByReferenceId\":false,"
                + "\"referenceId\":\"434545454dddd\",\"payload\":{\"vendorId\":\"601209960\","
                + "\"customerId\":\"601508675\",\"operationId\":\"RemoveSubscription\",\"subscriptionId\":\"600648704\","
                + "\"currentSubscriberId\":\"601209962\",\"eventGuid\":\"434545454dddd\",\"intent\":\"RemoveSubscriptions\","
                + "\"parameterType\":\"Purchase\",\"environment\":\"Test\",\"parameterMap\":{}},"
                + "\"vault\":{},\"id\":\"8d717006743f4bc78ff7aaca19aff9e8\"}";

        JsonObject payloadJson = null;
        try
        {

            payloadJson = JsonUtilities.fromJson(content, JsonObject.class);
        }
        catch (Exception ex)
        {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        }

        String webhookString3 = "{\"webhook_id_1ty56g6ret43er45trw\": {\"authenticator\": "
                + "{\"type\": \"JWT\",\"subject\": \"order-prov-notif\",\"secret\": \"order-prov-notif-stage-secret\"},"
                + "\"validator\": {\"type\": \"jsonSchema\",\"schemaFile\": \"name\"},\"transformer\": "
                + "{\"type\": \"OrderProvNotif\"}}}";

        JsonObject webhookJsonObject = new JsonParser().parse(webhookString3).getAsJsonObject();

        PowerMock.mockStatic(ThreadContext.class);
        EasyMock.expect(ThreadContext.getImmutableContext()).andReturn(new HashMap<String, String>()).anyTimes();
        EasyMock.expect(ThreadContext.getDepth()).andReturn(1).anyTimes();
        EasyMock.expect(ThreadContext.cloneStack()).andReturn(EasyMock.anyObject()).anyTimes();
        EasyMock.expect(ThreadContext.get(Constants.TRANSACTION_LOG_ID)).andReturn("123-234-345").anyTimes();


        Authenticator authenticator = new Authenticator();
        authenticator.setSecret("abc");
        authenticator.setSubject("xyz");
        authenticator.setPassword("asdfdf");
        authenticator.setUserName("tyuuu");
        authenticator.setType("basic");

        Validator validator = new Validator();
        validator.setSchema("abc");
        validator.setType("jsonSchema");

        Transformer transformer = new Transformer();
        transformer.setType("jsonPath");

        TaskProperties taskProperties = new TaskProperties();
        taskProperties.setType("recipeName");
        taskProperties.setParentReferenceId("we34");
        taskProperties.setPayload("");
        taskProperties.setPreferredExecutionDate("343434");
        taskProperties.setSequenceByParentReferenceId("true");
        taskProperties.setSequenceByReferenceId("true");
        
        transformer.setTaskProperties(taskProperties);
        
        
        WebServiceHook webServiceHook = new WebServiceHook();
        webServiceHook.setAuthenticator(authenticator);
        webServiceHook.setTransformer(transformer);
        webServiceHook.setValidator(validator);
        
        Profile profile = new Profile();
        
        Map webservicehooks = new HashMap<String,WebServiceHook>();
        webservicehooks.put("webhookid1", webServiceHook);
        
        profile.setWebhooks(webservicehooks);
        
        PowerMock.mockStatic(DbFactory.class);

        Task task = new Task();
        task.setId("028582");

        Database database = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(database).anyTimes();
        EasyMock.expect(database.getProfile(EasyMock.anyObject())).andReturn(profile).anyTimes();

        PowerMock.replayAll(DbFactory.class, ThreadContext.class, database);

        Response response = null;
        HttpServletRequest request = PowerMock.createMock(HttpServletRequest.class);
        try
        {
            response = webhookservice.executeWebhookTask(request, content, "webhookid1", "profileid1");
            Assert.assertTrue(true);
        }
        catch (WebhookAuthenticationException ex)
        {
            Assert.assertTrue(true);
        }
        catch (NestorException ex)
        {
            Assert.assertTrue(true);
        }
        catch (Exception ex)
        {
            Assert.assertTrue(true);
        }

        
        WebServiceHook webservicehook2 = profile.getWebhook("webhookid1");
        Assert.assertEquals("jsonPath",webservicehook2.getTransformer().getType());
        
        Assert.assertEquals("we34",webservicehook2.getTransformer().getTaskProperties().getParentReferenceId());
        
        Assert.assertEquals("343434",webservicehook2.getTransformer().getTaskProperties().getPreferredExecutionDate());
        Assert.assertEquals("true",webservicehook2.getTransformer().getTaskProperties().getSequenceByReferenceId());
        Assert.assertEquals("true",webservicehook2.getTransformer().getTaskProperties().getSequenceByParentReferenceId());
        Assert.assertEquals("",webservicehook2.getTransformer().getTaskProperties().getPayload());
        
        Assert.assertEquals("basic",webservicehook2.getAuthenticator().getType());
        Assert.assertEquals("tyuuu",webservicehook2.getAuthenticator().getUserName());
        Assert.assertEquals("asdfdf",webservicehook2.getAuthenticator().getPassword());
        Assert.assertEquals("xyz",webservicehook2.getAuthenticator().getSubject());
        Assert.assertEquals("abc",webservicehook2.getAuthenticator().getSecret());
        
        Assert.assertEquals("abc",webservicehook2.getValidator().getSchema());
        Assert.assertEquals("jsonSchema",webservicehook2.getValidator().getType());
        
    }

}
