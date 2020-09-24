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

package com.ibm.digital.mp.nestor.antilles.status;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.antilles.status.db.MongoHealthChecker;
import com.ibm.digital.mp.nestor.antilles.status.pingtest.AntillesPingHealthChecker;
import com.ibm.digital.mp.nestor.status.HealthChecker;

/**
 * This is the health checker for Antilles. It checks the following dependencies
 * <ul>
 * <li>Cloudant</li>
 * <li>Bluemix IAM</li>
 * <li>Bluemix BSS Resource Controller</li>
 * <li>API Connect for SSM Apis - TODO with Story 90677</li>
 * </ul>
 *
 */
public class AntillesHealthChecker implements HealthChecker
{

    private static final Logger logger = LogManager.getLogger(AntillesHealthChecker.class);

    private JsonObject health = new JsonObject();

    public AntillesHealthChecker()
    {
        health.addProperty(PARAM_HEALTHY, true);
        health.add(PARAM_DEPENDENCIES, new JsonArray());
    }

    @Override
    public JsonObject getHealth()
    {
        // For each health checker, obtain the current health and append dependencies
        boolean currentHealthyValue = health.get(PARAM_HEALTHY).getAsBoolean();
        for (HealthChecker healthChecker : getHealthCheckers())
        {
            JsonObject currentComponentHealth = healthChecker.getHealth();
            currentHealthyValue = currentHealthyValue && currentComponentHealth.get(PARAM_HEALTHY).getAsBoolean();
            if (currentComponentHealth.has(PARAM_DEPENDENCIES))
            {
                health.get(PARAM_DEPENDENCIES).getAsJsonArray().addAll(currentComponentHealth.get(PARAM_DEPENDENCIES).getAsJsonArray());
            }
        }
        health.addProperty(PARAM_HEALTHY, currentHealthyValue);
        logger.debug("Mongo Health Check = {}", currentHealthyValue);
        return health;
    }

    protected List<HealthChecker> getHealthCheckers()
    {
        List<HealthChecker> healthCheckers = new ArrayList<>();
        healthCheckers.add(new AntillesPingHealthChecker());
        healthCheckers.add(new MongoHealthChecker());
        return healthCheckers;
    }
}
