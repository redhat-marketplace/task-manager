package com.ibm.digital.mp.nestor.antilles.tasks.web.filters;

import javax.ws.rs.container.ContainerRequestContext;

import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.ibm.digital.mp.nestor.config.NestorConfiguration;
import com.ibm.digital.mp.nestor.config.NestorConfigurationFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ContainerRequestContext.class,NestorConfigurationFactory.class })
public class MaintenanceFilterTest
{
    @Test
    public void testFilterWithFlagOn() throws Exception
    {
        PowerMock.mockStatic(NestorConfigurationFactory.class);
        NestorConfigurationFactory factory = PowerMock.createMock(NestorConfigurationFactory.class);
        NestorConfiguration config = PowerMock.createMock(NestorConfiguration.class);
        EasyMock.expect(NestorConfigurationFactory.getInstance()).andReturn(factory).anyTimes();
        EasyMock.expect(factory.getNestorConfiguration()).andReturn(config).anyTimes();
        EasyMock.expect(config.maintenanceOn()).andReturn(true).anyTimes();
        ContainerRequestContext requestContext = PowerMock.createNiceMock(ContainerRequestContext.class);
        PowerMock.replayAll(NestorConfiguration.class, NestorConfigurationFactory.class,requestContext, factory, config);

        MaintenanceFilter realMaintenanceFilter = new MaintenanceFilter(){};
        realMaintenanceFilter.filter(requestContext);

    }
    
    @Test
    public void testFilterWithFlagOff() throws Exception
    {
        PowerMock.mockStatic(NestorConfigurationFactory.class);
        NestorConfigurationFactory factory = PowerMock.createMock(NestorConfigurationFactory.class);
        NestorConfiguration config = PowerMock.createMock(NestorConfiguration.class);
        EasyMock.expect(NestorConfigurationFactory.getInstance()).andReturn(factory).anyTimes();
        EasyMock.expect(factory.getNestorConfiguration()).andReturn(config).anyTimes();
        EasyMock.expect(config.maintenanceOn()).andReturn(false).anyTimes();
        ContainerRequestContext requestContext = PowerMock.createNiceMock(ContainerRequestContext.class);
        PowerMock.replayAll(NestorConfiguration.class, NestorConfigurationFactory.class, requestContext,factory, config);

        MaintenanceFilter realMaintenanceFilter = new MaintenanceFilter(){};
        realMaintenanceFilter.filter(requestContext);

    }

}
