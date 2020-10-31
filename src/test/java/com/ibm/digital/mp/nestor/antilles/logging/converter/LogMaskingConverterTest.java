package com.ibm.digital.mp.nestor.antilles.logging.converter;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.Assert;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class LogMaskingConverterTest
{

    @Test
    public void testMasking()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("encode_apiKey", "test");
        vault.addProperty("encode_accountId", "test");
        vault.addProperty("email", "saumya8@mailinator.com");
        vault.addProperty("test1", "test1");
        vault.addProperty("homePhone", "4434343434");
        vault.addProperty("ContactPhoneNumber", "test1");
        vault.addProperty("asis", "asis");
        vault.addProperty("partNumber", "test_trial");
        StringBuilder outputMessage = new StringBuilder();
        Message message = new SimpleMessage(vault.toString());

        LogEvent event = Log4jLogEvent.newBuilder().setLevel(Level.INFO).setMessage(message).build();
        LogMaskingConverter logMask = new LogMaskingConverter("test", "test");
        logMask.format(event, outputMessage);
        JsonObject output = new Gson().fromJson(outputMessage.toString().split("\n")[0], JsonObject.class);
        Assert.assertEquals("********", output.get("encode_apiKey").getAsString());
        Assert.assertEquals("********", output.get("homePhone").getAsString());
        Assert.assertNotEquals("test_trial", output.get("partNumber").getAsString());
        Assert.assertEquals("asis", output.get("asis").getAsString());
    }
}
