package com.ibm.digital.mp.nestor.internal.antilles.tasks.recipes;

import java.net.URL;

import javax.servlet.ServletContext;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.ibm.digital.mp.nestor.internal.util.StringContainsClassRestrictor;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.tasks.recipes.Recipe;

public class RecipeManagerTest
{
    @Test
    public void testGetInstance()
    {
        RecipeManager rm1 = RecipeManager.getInstance();
        RecipeManager rm2 = RecipeManager.getInstance();
        Assert.assertTrue(rm1 == rm2);
    }

    @Test
    public void testSetAppServletContext()
    {
        ServletContext sc1 = Mockito.mock(ServletContext.class);
        ServletContext sc2 = Mockito.mock(ServletContext.class);
        RecipeManager rm = new RecipeManager();
        rm.setAppServletContext(sc1);
        rm.setAppServletContext(sc2);
    }

    @Test
    public void testGetRecipeWithNullRecipe()
    {
        RecipeManager recipeManager = new RecipeManager();
        Profile profile = new Profile();
        Assert.assertNull(recipeManager.getRecipe("DummyRecipe", profile));
    }

    @Test
    public void testGetUnknownRecipe()
    {
        RecipeManager rm = new RecipeManager();
        ServletContext servletContext = Mockito.mock(ServletContext.class);
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        Mockito.when(servletContext.getClassLoader()).thenReturn(classLoader);
        rm.setAppServletContext(servletContext);
        String id = "recipe1";
        Profile profile = Mockito.mock(Profile.class);
        String recipeClass = "com.nothing.doesnt.exist.WhatClass";
        Mockito.when(profile.getRecipe(Mockito.eq(id))).thenReturn(recipeClass);
        Recipe recipe = rm.getRecipe(id, profile);
        Assert.assertNull(recipe);
    }

    @Test
    public void testGetRecipeNullProfile()
    {
        RecipeManager rm = new RecipeManager();
        try
        {
            rm.getRecipe("id", null);
            Assert.fail("Expected failure.");
        }
        catch (Exception ex)
        {
            Assert.assertTrue(ex instanceof NullPointerException);
        }
    }

    @Test
    public void testGetRestrictedClassLoader()
    {
        try
        {
            RecipeManager rm = new RecipeManager();
            ServletContext servletContext = Mockito.mock(ServletContext.class);
            URL url = new URL("http://www.google.com");
            Mockito.when(servletContext.getResource(Mockito.anyString())).thenReturn(url);
            Mockito.when(servletContext.getClassLoader()).thenReturn(ClassLoader.getSystemClassLoader());
            rm.setAppServletContext(servletContext);
            Profile profile = Mockito.mock(Profile.class);
            String recipeLib = "abc.jar";
            Mockito.when(profile.getRecipeLibraryFile()).thenReturn(recipeLib);
            ClassLoader rcl = rm.getRestrictedClassloader(profile);
            Assert.assertTrue(rcl instanceof StringContainsClassRestrictor);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void testNullSetAppServletContext()
    {
        try
        {
            RecipeManager rm = new RecipeManager();
            rm.setAppServletContext(null);
        }
        catch (Exception ex)
        {
            Assert.fail(ex.getMessage());
        }
    }
}
