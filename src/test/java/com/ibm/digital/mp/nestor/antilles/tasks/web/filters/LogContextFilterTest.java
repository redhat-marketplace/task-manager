package com.ibm.digital.mp.nestor.antilles.tasks.web.filters;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;

import org.easymock.EasyMock;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.support.membermodification.MemberModifier;

import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.util.Constants;

public class LogContextFilterTest
{
    @Test
    public void testFilterWithRequestContext() throws Exception
    {

        ContainerRequestContext requestContext = PowerMock.createMock(ContainerRequestContext.class);
        HttpServletRequest servletRequest = PowerMock.createMock(HttpServletRequest.class);
        Profile profile = new Profile();
        LogContextFilter logContextFilter = new LogContextFilter();
        MemberModifier.field(LogContextFilter.class, "servletRequest").set(logContextFilter, servletRequest);
        EasyMock.expect(requestContext.getProperty("profile")).andReturn(profile).anyTimes();
        EasyMock.expect(requestContext.getHeaderString(Constants.TRANSACTION_ID_HEADER)).andReturn("123").anyTimes();
        EasyMock.expect(servletRequest.getRemoteAddr()).andReturn("123").anyTimes();
        EasyMock.expect(servletRequest.getRemoteHost()).andReturn("123").anyTimes();
        EasyMock.expect(servletRequest.getRequestURI()).andReturn("123").anyTimes();
        EasyMock.expect(servletRequest.getQueryString()).andReturn("123").anyTimes();
        System.setProperty("ENVIRONMENT", "PROD");
        PowerMock.replayAll(servletRequest, requestContext);

        logContextFilter.filter(requestContext);
        System.clearProperty("ENVIRONMENT");
    }

    @Test
    public void testFilterWithRequestContextException() throws Exception
    {

        ContainerRequestContext requestContext = PowerMock.createMock(ContainerRequestContext.class);
        HttpServletRequest servletRequest = PowerMock.createMock(HttpServletRequest.class);
        Profile profile = new Profile();
        LogContextFilter logContextFilter = new LogContextFilter();
        MemberModifier.field(LogContextFilter.class, "servletRequest").set(logContextFilter, servletRequest);
        EasyMock.expect(requestContext.getProperty("profile")).andReturn(profile).anyTimes();
        EasyMock.expect(servletRequest.getRemoteAddr()).andReturn("123").anyTimes();
        EasyMock.expect(servletRequest.getRemoteHost()).andReturn("123").anyTimes();
        EasyMock.expect(servletRequest.getRequestURI()).andThrow(new IllegalArgumentException("")).anyTimes();
        PowerMock.replayAll(servletRequest, requestContext);

        logContextFilter.filter(requestContext);
    }

    @Test
    public void testFilter() throws Exception
    {

        ContainerRequestContext requestContext = PowerMock.createMock(ContainerRequestContext.class);

        LogContextFilter logContextFilter = new LogContextFilter();

        logContextFilter.filter(requestContext, null);
    }

    @Test
    public void testFilterWithRequestContextEmptyTransactionId() throws Exception
    {

        ContainerRequestContext requestContext = PowerMock.createMock(ContainerRequestContext.class);
        HttpServletRequest servletRequest = PowerMock.createMock(HttpServletRequest.class);
        Profile profile = new Profile();
        LogContextFilter logContextFilter = new LogContextFilter();
        MemberModifier.field(LogContextFilter.class, "servletRequest").set(logContextFilter, servletRequest);
        EasyMock.expect(requestContext.getProperty("profile")).andReturn(profile).anyTimes();
        EasyMock.expect(requestContext.getHeaderString(Constants.TRANSACTION_ID_HEADER)).andReturn(null).anyTimes();
        EasyMock.expect(servletRequest.getRemoteAddr()).andReturn("123").anyTimes();
        EasyMock.expect(servletRequest.getRemoteHost()).andReturn("123").anyTimes();
        EasyMock.expect(servletRequest.getRequestURI()).andReturn("123").anyTimes();
        EasyMock.expect(servletRequest.getQueryString()).andReturn("123").anyTimes();
        System.setProperty("ENVIRONMENT", "PROD");
        PowerMock.replayAll(servletRequest, requestContext);

        logContextFilter.filter(requestContext);
        System.clearProperty("ENVIRONMENT");
    }

}
