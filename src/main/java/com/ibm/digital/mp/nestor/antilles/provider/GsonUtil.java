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

package com.ibm.digital.mp.nestor.antilles.provider;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class GsonUtil
{
    public GsonUtil()
    {
        //
    }

    public static final String PATTERN = "DD/MM/YY HH:mm:ss";

    private Gson gson;
    private Gson gsonExpose;
    private SimpleDateFormat sdf;

    /**
     * Fetches an instance of {@link Gson}.
     * 
     * @return A Gson instance
     */
    public Gson getInstance()
    {
        if (gson == null)
        {
            gson = getGsonBuilderInstance(false).create();
        }
        return gson;
    }

    /**
     * Fetches an instance of {@link Gson}.
     * 
     * @param onlyExpose
     *            If only exposed variables should be serialized
     * @return A Gson instance
     */
    public Gson getInstance(boolean onlyExpose)
    {
        if (!onlyExpose)
        {
            if (gson == null)
            {
                gson = getGsonBuilderInstance(false).create();
            }
            return gson;
        }
        else
        {
            if (gsonExpose == null)
            {
                gsonExpose = getGsonBuilderInstance(true).create();
            }
            return gsonExpose;
        }
    }

    /**
     * A {@link Gson} instance with expose as true.
     * 
     * @return The Gson instance
     */
    public Gson getExposeInstance()
    {
        if (gsonExpose == null)
        {
            gsonExpose = getGsonBuilderInstance(true).create();
        }
        return gsonExpose;
    }

    /**
     * The date format to use.
     * 
     * @return {@link SimpleDateFormat} for JSON dates
     */
    public SimpleDateFormat getSdfInstance()
    {
        if (sdf == null)
        {
            sdf = new SimpleDateFormat(PATTERN);
        }
        return sdf;
    }

    private GsonBuilder getGsonBuilderInstance(boolean onlyExpose)
    {
        GsonBuilder gsonBuilder = new GsonBuilder();
        if (onlyExpose)
        {
            gsonBuilder.excludeFieldsWithoutExposeAnnotation();
        }
        gsonBuilder.registerTypeAdapter(Date.class, new JsonDeserializer<Date>()
        {
            @Override
            public Date deserialize(JsonElement json, Type type, JsonDeserializationContext arg2) throws JsonParseException
            {
                try
                {
                    return getSdfInstance().parse(json.getAsString());
                }
                catch (ParseException ex)
                {
                    return null;
                }
            }

        });
        gsonBuilder.registerTypeAdapter(Date.class, new JsonSerializer<Date>()
        {
            @Override
            public JsonElement serialize(Date src, Type typeOfSrc, JsonSerializationContext context)
            {
                return src == null ? null : new JsonPrimitive(getSdfInstance().format(src));
            }
        });
        return gsonBuilder;
    }

    /**
     * Convert from JSON string to an object.
     * 
     * @param json
     *            The JSON as String
     * @param classOfT
     *            The class to convert to
     * @param onlyExpose
     *            <code>true</code> if only exposed variables are to be populated
     * @return The converted object
     */
    public <T> T fromJson(String json, Class<T> classOfT, boolean onlyExpose)
    {
        try
        {
            return getInstance(onlyExpose).fromJson(json, classOfT);
        }
        catch (Exception ex)
        {
            // Log exception
            return null;
        }
    }

    /**
     * Create a deep copy of an object.
     * 
     * @param object
     *            The object to clone.
     * @param type
     *            The type of the object.
     * @return A deep copy of the object.
     */
    public static <T> T deepCopy(T object, Class<T> type)
    {
        try
        {
            Gson gson = new Gson();
            return gson.fromJson(gson.toJson(object, type), type);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
}
