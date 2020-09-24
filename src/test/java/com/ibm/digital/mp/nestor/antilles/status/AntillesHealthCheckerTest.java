package com.ibm.digital.mp.nestor.antilles.status;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.antilles.status.db.MongoHealthChecker;
import com.ibm.digital.mp.nestor.antilles.status.pingtest.AntillesPingHealthChecker;
import com.ibm.digital.mp.nestor.config.NestorConfiguration;
import com.ibm.digital.mp.nestor.config.NestorConfigurationFactory;
import com.ibm.digital.mp.nestor.status.HealthChecker;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ NestorConfigurationFactory.class, NestorConfiguration.class })
public class AntillesHealthCheckerTest
{
    @Test
    public void testGetHealthCheckers()
    {
        AntillesHealthChecker healthChecker = new AntillesHealthChecker();
        List<HealthChecker> healtherCheckers = healthChecker.getHealthCheckers();
        Assert.assertTrue(healtherCheckers.get(0) instanceof AntillesPingHealthChecker);
        Assert.assertTrue(healtherCheckers.get(1) instanceof MongoHealthChecker);
    }

    @Test
    public void testGetHealth()
    {
        // Antilles Health Checker with more than one health checker
        HealthChecker healthChecker = new HealthChecker()
        {

            @Override
            public JsonObject getHealth()
            {
                JsonObject health = new JsonObject();
                health.addProperty("healthy", true);
                health.add("dependencies", new JsonArray());
                return health;
            }
        };

        AntillesHealthChecker antillesHealthChecker = new AntillesHealthChecker()
        {
            @Override
            protected List<HealthChecker> getHealthCheckers()
            {
                List<HealthChecker> healthCheckers = new ArrayList<>();
                healthCheckers.add(healthChecker);
                return healthCheckers;
            }
        };
        Assert.assertTrue(antillesHealthChecker.getHealth().get("healthy").getAsBoolean());

        // Antilles Health Checker with one health checker which fails
        HealthChecker healthChecker1 = new HealthChecker()
        {

            @Override
            public JsonObject getHealth()
            {
                JsonObject health = new JsonObject();
                health.addProperty("healthy", false);
                return health;
            }
        };

        antillesHealthChecker = new AntillesHealthChecker()
        {
            @Override
            protected List<HealthChecker> getHealthCheckers()
            {
                List<HealthChecker> healthCheckers = new ArrayList<>();
                healthCheckers.add(healthChecker1);
                healthCheckers.add(healthChecker);
                return healthCheckers;
            }
        };
        Assert.assertFalse(antillesHealthChecker.getHealth().get("healthy").getAsBoolean());

        // Antilles Health Checker with no health checkers
        antillesHealthChecker = new AntillesHealthChecker()
        {
            @Override
            protected List<HealthChecker> getHealthCheckers()
            {
                return new ArrayList<>();
            }
        };
        Assert.assertTrue(antillesHealthChecker.getHealth().get("healthy").getAsBoolean());

    }

}
