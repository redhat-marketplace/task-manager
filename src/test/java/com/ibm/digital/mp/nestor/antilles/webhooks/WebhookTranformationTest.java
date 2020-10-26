package com.ibm.digital.mp.nestor.antilles.webhooks;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.digital.mp.nestor.tasks.Transformer;

public class WebhookTranformationTest
{
    
    @Test
    public void testTransformTask() throws Exception
    {

        String transformString = "{\"type\": \"jsonPath\",\"taskProperties\":{\"type\": \"OrderProvNotif\",\"referenceId\":"
                + " \"RefId\",\"preferredExecutionDate\": \"1573551091\",\"sequenceByReferenceId\": \"true\","
                + "\"sequenceByParentReferenceId\": \"true\"," + "\"parentReferenceId\": \"RefId\"," + "\"payload\": \"$.\"} }";

        JsonObject trasnformJsonObject = new JsonParser().parse(transformString).getAsJsonObject();

        Gson json = new Gson();
        Transformer transformer = json.fromJson(trasnformJsonObject, Transformer.class);

        String payload = "{\"orderType\": \"orderhub\",\"orderId\": \"sss345rt5\"}";

        TransformedTask transformedTask = new WebhookTransformation().transformTask(transformer, payload);

        Assert.assertEquals("RefId", transformedTask.getParentReferenceId());
        Assert.assertEquals("true", transformedTask.getSequenceByParentReferenceId());
        Assert.assertEquals("true", transformedTask.getSequenceByReferenceId());
        Assert.assertEquals("1573551091", String.valueOf(transformedTask.getPreferredExecutionDate()));
        Assert.assertEquals("OrderProvNotif", transformedTask.getType());
        Assert.assertEquals("RefId", transformedTask.getReferenceId());
        Assert.assertEquals(payload, transformedTask.getPayload());
    }
    
    @Test
    public void testTransformTask_jsonpath() throws Exception
    {

        String transformString = "{\"type\": \"jsonPath\",\"taskProperties\":{\"type\": \"$.operation\",\"referenceId\":"
                + " \"RefId\",\"preferredExecutionDate\": \"1573551091\",\"sequenceByReferenceId\": \"true\","
                + "\"sequenceByParentReferenceId\": \"true\"," + "\"parentReferenceId\": \"RefId\"," + "\"payload\": \"$.data\"} }";

        JsonObject trasnformJsonObject = new JsonParser().parse(transformString).getAsJsonObject();

        Gson json = new Gson();
        Transformer transformer = json.fromJson(trasnformJsonObject, Transformer.class);

        String payload = "{\"operation\": \"OrderProvNotif\",\"data\":{\"orderType\": \"orderhub\",\"orderId\": \"sss345rt5\"}}";

        TransformedTask transformedTask = new WebhookTransformation().transformTask(transformer, payload);

        Assert.assertEquals("RefId", transformedTask.getParentReferenceId());
        Assert.assertEquals("true", transformedTask.getSequenceByParentReferenceId());
        Assert.assertEquals("true", transformedTask.getSequenceByReferenceId());
        Assert.assertEquals("1573551091", String.valueOf(transformedTask.getPreferredExecutionDate()));
        Assert.assertEquals("OrderProvNotif", transformedTask.getType());
        Assert.assertEquals("RefId", transformedTask.getReferenceId());
        
        TransformedTask transformedTask1 = new TransformedTask();
        transformedTask1.setPreferredExecutionDate(1234567L);
        transformedTask1.setParentReferenceId("refId");
        transformedTask1.setPayload("payload");
        transformedTask1.setReferenceId("refId");
        transformedTask1.setSequenceByParentReferenceId("true");
        transformedTask1.setSequenceByReferenceId("true");
        transformedTask1.setType("orderProvNotif");
        
        Assert.assertEquals("refId", transformedTask1.getParentReferenceId());
        Assert.assertEquals("true", transformedTask1.getSequenceByParentReferenceId());
        Assert.assertEquals("true", transformedTask1.getSequenceByReferenceId());
        Assert.assertEquals("1234567", String.valueOf(transformedTask1.getPreferredExecutionDate()));
        Assert.assertEquals("orderProvNotif", transformedTask1.getType());
        Assert.assertEquals("refId", transformedTask1.getReferenceId());
        
    }
    
    @Test
    public void testTransformTask_Wrong_jsonpath() throws Exception
    {

        String transformString = "{\"type\": \"java\",\"taskProperties\":{\"type\": \"$.operation\",\"referenceId\":"
                + " \"RefId\",\"preferredExecutionDate\": \"1573551091\",\"sequenceByReferenceId\": \"true\","
                + "\"sequenceByParentReferenceId\": \"true\"," + "\"parentReferenceId\": \"RefId\"," + "\"payload\": \"$.data\"} }";

        JsonObject trasnformJsonObject = new JsonParser().parse(transformString).getAsJsonObject();

        Gson json = new Gson();
        Transformer transformer = json.fromJson(trasnformJsonObject, Transformer.class);

        String payload = "{\"operation\": \"OrderProvNotif\",\"data\":{\"orderType\": \"orderhub\",\"orderId\": \"sss345rt5\"}}";

        try
        {
            TransformedTask transformedTask = new WebhookTransformation().transformTask(transformer, payload);
            Assert.assertTrue(true);
        }
        catch (Exception ex)
        {
            Assert.assertTrue(true);
            WebhookTransformationException webhookTransformationException1 = new WebhookTransformationException(ex);
            WebhookTransformationException webhookTransformationException2 = new WebhookTransformationException(ex, "NS23456");
            WebhookTransformationException webhookTransformationException3 = new WebhookTransformationException("transform error", ex,
                    "NS23456");
            WebhookTransformationException webhookTransformationException4 = new WebhookTransformationException("transform error", ex);
        }

        WebhookTransformationException webhookTransformationException = new WebhookTransformationException("transform error","NS12345");
        Assert.assertEquals("NS12345",webhookTransformationException.getErrorCode());
        
        WebhookTransformationException webhookTransformationException5 = new WebhookTransformationException("transform error");
    }
    
    
    
    @Test
    public void testWebhookException() throws Exception
    {

        String transformString = "{\"type\": \"java\",\"taskProperties\":{\"type\": \"$.operation\",\"referenceId\":"
                + " \"RefId\",\"preferredExecutionDate\": \"1573551091\",\"sequenceByReferenceId\": \"true\","
                + "\"sequenceByParentReferenceId\": \"true\"," + "\"parentReferenceId\": \"RefId\"," + "\"payload\": \"$.data\"} }";

        JsonObject trasnformJsonObject = new JsonParser().parse(transformString).getAsJsonObject();

        Gson json = new Gson();
        Transformer transformer = json.fromJson(trasnformJsonObject, Transformer.class);

        String payload = "{\"operation\": \"OrderProvNotif\",\"data\":{\"orderType\": \"orderhub\",\"orderId\": \"sss345rt5\"}}";

        try
        {
            TransformedTask transformedTask = new WebhookTransformation().transformTask(transformer, payload);
            Assert.assertTrue(true);
        }
        catch (Exception ex)
        {
            Assert.assertTrue(true);
            
            WebhookException webhookException = new WebhookAuthenticationException("webhook auth exception");
            Assert.assertEquals("webhook auth exception",webhookException.getMessage());
            
            WebhookException webhookException1 = new WebhookAuthenticationException("webhook auth exception",ex);
            
            WebhookException webhookException2 = new WebhookAuthenticationException("webhook auth exception",ex,"NS2345");
            Assert.assertEquals("NS2345",webhookException2.getErrorCode());
            
            WebhookException webhookException3 = new WebhookAuthenticationException(ex,"NS2345");
            Assert.assertEquals("NS2345",webhookException3.getErrorCode());
            
            WebhookException webhookException4 = new WebhookAuthenticationException("webhook auth exception","NS2345");
            Assert.assertEquals("webhook auth exception",webhookException4.getMessage());
            
            WebhookException webhookException5 = new WebhookAuthenticationException(ex);
            
        }

        WebhookTransformationException webhookTransformationException = new WebhookTransformationException("transform error","NS12345");
        Assert.assertEquals("NS12345",webhookTransformationException.getErrorCode());
        
        WebhookTransformationException webhookTransformationException5 = new WebhookTransformationException("transform error");
        
        
        TransformedTask transformedTask = new TransformedTask();
        transformedTask.setPreferredExecutionDate(1234567L);
        transformedTask.setParentReferenceId("refId");
        transformedTask.setPayload("payload");
        transformedTask.setReferenceId("refId");
        transformedTask.setSequenceByParentReferenceId("true");
        transformedTask.setSequenceByReferenceId("true");
        
    }

}
