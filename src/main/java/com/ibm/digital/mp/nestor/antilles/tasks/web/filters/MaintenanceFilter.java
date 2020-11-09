package com.ibm.digital.mp.nestor.antilles.tasks.web.filters;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.ext.Provider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ibm.digital.mp.nestor.antilles.util.TaskWebUtil;
import com.ibm.digital.mp.nestor.config.NestorConfigurationFactory;

@Maintenance
@Priority(1)
@Provider
public class MaintenanceFilter implements ContainerRequestFilter

{
    private static final Logger logger = LogManager.getLogger(MaintenanceFilter.class);

    @Override
    public void filter(ContainerRequestContext requestContext)
    {
        if (NestorConfigurationFactory.getInstance().getNestorConfiguration().maintenanceOn())
        {
            logger.info("Maintenance window is ON ");
            requestContext.abortWith(TaskWebUtil.buildServerErrorResponse("NEST7124E",
                    "Maintenance window is ON right now. It might take few hours. Please wait.."));
        }

    }
}
