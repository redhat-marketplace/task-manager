package com.ibm.digital.mp.nestor.antilles.tasks.web;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.status.SimplePingHealthChecker;

public class StatusWebServiceTest
{
    @Test
    public void testPing()
    {
        StatusWebService sws = new StatusWebService();
        Response response = sws.ping();
        Assert.assertEquals(200, response.getStatus());
    }

    @Test
    public void testGetHealth()
    {
        StatusWebService sws = new StatusWebService()
        {
            @Override
            protected SimplePingHealthChecker getHealthChecker()
            {
                return new SimplePingHealthChecker()
                {
                    @Override
                    public JsonObject getHealth()
                    {
                        JsonObject health = new JsonObject();
                        health.addProperty("test", "value");
                        return health;
                    }
                };
            }
        };
        Response response = sws.getHealth();
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("{\"test\":\"value\"}", response.getEntity());
        Assert.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
    }

    @Test
    public void testGetHealthChecker()
    {
        Assert.assertNotNull(new StatusWebService().getHealthChecker());
    }
}
