package com.ibm.digital.mp.nestor.antilles.provider;

import org.junit.Assert;
import org.junit.Test;

import com.ibm.digital.mp.nestor.tasks.Task;

public class GsonUtilTest
{

    @Test
    public void testGetInstanceWithExposeTrue()
    {
        GsonUtil gu = new GsonUtil();
        Assert.assertNotNull(gu.getInstance(true));
    }

    @Test
    public void testGetInstanceWithExposeFalse()
    {
        GsonUtil gu = new GsonUtil();
        Assert.assertNotNull(gu.getInstance(false));
    }

    @Test
    public void testGetInstance()
    {
        GsonUtil gu = new GsonUtil();
        Assert.assertNotNull(gu.getInstance());
    }

    @Test
    public void testGetInstanceWithExposefalseAndGsonNotNull()
    {
        GsonUtil gu = new GsonUtil();
        Assert.assertNotNull(gu.getInstance(true));
    }

    @Test
    public void testGetExposeInstanceWithExposeNotNull()
    {
        GsonUtil gu = new GsonUtil();
        gu.getInstance(true);
        Assert.assertNotNull(gu.getExposeInstance());
    }

    @Test
    public void testGetExposeInstance()
    {
        GsonUtil gu = new GsonUtil();
        Assert.assertNotNull(gu.getExposeInstance());
    }

    @Test
    public void testGetSdfInstance()
    {
        GsonUtil gu = new GsonUtil();
        Assert.assertNotNull(gu.getSdfInstance());
        Assert.assertNotNull(gu.getSdfInstance());
    }

    @Test
    public void testFromJson()
    {
        GsonUtil gu = new GsonUtil();
        Assert.assertNotNull(gu.fromJson("{\"key\":\"value\"}", Task.class, false));
    }

    @Test
    public void testFromJsonThrowNull()
    {
        GsonUtil gu = new GsonUtil();
        Assert.assertNull(gu.fromJson("{\"key\":\"value\"}", String.class, false));
    }

    @Test
    public void testDeepCopy()
    {
        Assert.assertNotNull(GsonUtil.deepCopy(new Task(), Task.class));
    }

    @Test
    public void testDeepCopyException()
    {
        Assert.assertNull(GsonUtil.deepCopy(new Task(), null));
    }

}
