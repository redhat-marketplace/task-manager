/********************************************************** {COPYRIGHT-TOP} ****
 * IBM Internal Use Only
 * IBM Marketplace
 *
 * (C) Copyright IBM Corp. 2017  All Rights Reserved.
 *
 * The source code for this program is not published or otherwise  
 * divested of its trade secrets, irrespective of what has been 
 * deposited with the U.S. Copyright Office.
 ********************************************************** {COPYRIGHT-END} ***/

package com.ibm.digital.mp.nestor.internal.antilles.tasks.recipes;

import java.net.MalformedURLException;
import java.net.URL;

import javax.servlet.ServletContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ibm.digital.mp.nestor.internal.util.StringContainsClassRestrictor;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.tasks.recipes.Recipe;

public class RecipeManager
{
    private Logger logger = LogManager.getLogger(RecipeManager.class);

    private static class Loader
    {
        private static RecipeManager instance;

        static
        {
            instance = new RecipeManager();
        }
    }

    private static final String RECIPE_LIBRARY_PATH = "lib-recipes";
    private static final String[] RESTRICTED_PACKAGES = { "nestor.internal" };
    private ServletContext servletContext;

    public static RecipeManager getInstance()
    {
        return Loader.instance;
    }

    /**
     * Set the app servlet context.
     * 
     * @param servletContext
     *            The context
     */
    public void setAppServletContext(ServletContext servletContext)
    {
        if (servletContext != null)
        {
            this.servletContext = servletContext;
        }
    }

    /**
     * Retrieves a recipe for the id.
     * 
     * @param id
     *            The task
     * @param profile
     *            The profile against which it will be loaded
     * @return the recipe
     */
    public Recipe getRecipe(String id, Profile profile)
    {
        String recipeClass = profile.getRecipe(id);
        if (recipeClass != null)
        {
            try
            {
                return (Recipe) servletContext.getClassLoader().loadClass(recipeClass).newInstance();
            }
            catch (Throwable th)
            {
                logger.error("Failed to instantiate recipe with id [" + id + "] and class [" + recipeClass + "]", th);
                return null;
            }
        }
        return null;
    }

    /**
     * This is to load the restricted class loader. Logic to be revisited.
     * 
     * @param profile
     *            The current profile.
     * @return A class loader.
     * @throws MalformedURLException
     *             if the Jar file URL is malformed.
     */
    protected ClassLoader getRestrictedClassloader(Profile profile) throws MalformedURLException
    {
        StringContainsClassRestrictor recipeLoader = null;
        URL libUrl = servletContext.getResource("/WEB-INF/" + RECIPE_LIBRARY_PATH + "/" + profile.getRecipeLibraryFile());
        recipeLoader = new StringContainsClassRestrictor(new URL[] { libUrl }, servletContext.getClassLoader(), RESTRICTED_PACKAGES);
        return recipeLoader;
    }
}
