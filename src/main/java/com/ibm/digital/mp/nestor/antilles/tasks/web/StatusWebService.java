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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.ibm.digital.mp.nestor.antilles.status.AntillesHealthChecker;
import com.ibm.digital.mp.nestor.status.HealthChecker;

@Path("/")
public class StatusWebService
{
    @GET
    @Path("/ping")
    public Response ping()
    {
        return Response.ok().build();
    }

    @GET
    @Path("/health")
    public Response getHealth()
    {
        return Response.ok(getHealthChecker().getHealth().toString(), MediaType.APPLICATION_JSON).build();
    }

    protected HealthChecker getHealthChecker()
    {
        return new AntillesHealthChecker();
    }
}
