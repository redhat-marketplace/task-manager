package com.ibm.digital.mp.nestor.antilles.provider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.junit.Assert;
import org.junit.Test;

import com.ibm.digital.mp.nestor.tasks.Task;

public class GsonProviderTest
{

    GsonProvider gsonProvider = new GsonProvider();

    @Test
    public void testIsReadable() throws Exception
    {
        Assert.assertTrue(gsonProvider.isReadable(Task.class, null, null, null));
    }

    @Test
    public void testIsWritable() throws Exception
    {
        Assert.assertTrue(gsonProvider.isWriteable(Task.class, null, null, null));
    }

    @Test
    public void testGetSize() throws Exception
    {
        Assert.assertTrue(-1 == gsonProvider.getSize(Task.class, null, null, null, null));
    }

    @Test
    public void testReadFrom() throws Exception
    {
        byte[] bytes = "{\"file\":{\"list\":\"cdrive\"}}".getBytes();
        Object taskObj = gsonProvider.readFrom((Class) Task.class, Task.class, null, null, null, new ByteArrayInputStream(bytes));
        Assert.assertNotNull(taskObj);
    }

    @Test
    public void testReadFromException() throws Exception
    {
        byte[] bytes = "{\"file\"::\"cdrive\"}]}}".getBytes();
        gsonProvider.readFrom((Class) Task.class, Task.class, null, null, null, new ByteArrayInputStream(bytes));
        Assert.assertTrue("Exception should not be thrown", true);
    }

    @Test
    public void testWriteTo() throws Exception
    {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Task task = new Task();
        task.setType("DUMMY");
        task.setStatus(com.ibm.digital.mp.nestor.tasks.Status.COMPLETED);
        gsonProvider.writeTo(task, Task.class, Task.class, null, null, null, stream);
        Assert.assertTrue("Exception should not be thrown", true);
    }
}
