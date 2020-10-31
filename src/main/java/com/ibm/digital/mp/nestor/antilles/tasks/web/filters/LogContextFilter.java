package com.ibm.digital.mp.nestor.antilles.tasks.web.filters;

import java.io.IOException;

import javax.annotation.Priority;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import com.ibm.digital.mp.nestor.config.BluemixInfo;
import com.ibm.digital.mp.nestor.config.EnvironmentUtilities;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.util.Constants;

@Priority(Priorities.USER)
@Provider
public class LogContextFilter implements ContainerRequestFilter, ContainerResponseFilter
{
    private static final Logger logger = LogManager.getLogger(LogContextFilter.class);

    @Context
    private HttpServletRequest servletRequest;

    @Override
    public void filter(ContainerRequestContext requestContext)
    {
        try
        {
            Profile profile = (Profile) requestContext.getProperty(Constants.PROFILE);
            // Adding logging context
            ThreadContext.put("id", profile.getId());
            ThreadContext.put("ipAddress", servletRequest.getRemoteAddr());
            ThreadContext.put("hostName", servletRequest.getRemoteHost());
            ThreadContext.put("uri", servletRequest.getRequestURI());
            ThreadContext.put("query", servletRequest.getQueryString() + "");
            ThreadContext.put("env", EnvironmentUtilities.getEnvironmentName());
            ThreadContext.put("region", BluemixInfo.getRegion());
            ThreadContext.put("appInstance", BluemixInfo.getAppInstance());
            String transactionId = requestContext.getHeaderString(Constants.TRANSACTION_ID_HEADER);
            if (!org.apache.commons.lang3.StringUtils.isBlank(transactionId))
            {
                ThreadContext.put(Constants.TRANSACTION_LOG_ID, transactionId);
            }
        }
        catch (Exception ex)
        {
            logger.error(ex.getMessage(), ex);
        }
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException
    {
        ThreadContext.clearMap();
    }
}
