package com.ibm.digital.mp.nestor.antilles.webhooks;

import java.text.ParseException;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.digital.mp.nestor.api.NestorException;
import com.ibm.digital.mp.nestor.tasks.TaskProperties;
import com.ibm.digital.mp.nestor.tasks.Transformer;
import com.ibm.digital.mp.nestor.util.JsonUtilities;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

public class WebhookTransformation
{

    /**
     * This method is responsible to transform task in nestor.
     * 
     * @return created taskId.
     * @throws NestorException
     *             nestor exception.
     * @throws ParseException
     *             exception while reading date.
     * @throws WebhookTransformationException
     *             exception in transform.
     */
    public TransformedTask transformTask(Transformer transformer, String payload)
            throws NestorException, ParseException, WebhookTransformationException
    {

        JsonObject payloadJson = JsonUtilities.fromJson(payload, JsonObject.class);

        String transformerType = transformer.getType();

        TransformedTask transformedTask = null;

        if (transformerType.equals("jsonPath"))
        {
            TaskProperties taskProperties = transformer.getTaskProperties();

            JsonObject jsonTaskPropertiesResponse = new JsonObject(); // To create final jsonObject

            Configuration jsonPathConfig = Configuration.builder().build().addOptions(Option.SUPPRESS_EXCEPTIONS);
            DocumentContext jsonDocCtxt = JsonPath.parse(payloadJson.toString(), jsonPathConfig);

            Gson json = new Gson();
            JsonObject jsonTaskProperties = new JsonParser().parse(json.toJson(taskProperties)).getAsJsonObject();

            for (Entry<String, JsonElement> entry : jsonTaskProperties.entrySet())
            {
                try
                {
                    JsonPath path = JsonPath.compile(entry.getValue().getAsString());
                    Object match = jsonDocCtxt.read(path);
                    if (match != null)
                    {  // Json path specified in profile.
                        if (match instanceof Boolean)
                        {
                            jsonTaskPropertiesResponse.addProperty(entry.getKey(), (Boolean) match);
                        }
                        else if (match instanceof Number)
                        {
                            jsonTaskPropertiesResponse.addProperty(entry.getKey(), (Number) match);
                        }
                        else if (match instanceof String)
                        {
                            jsonTaskPropertiesResponse.addProperty(entry.getKey(), (String) match);
                        }
                        else
                        {
                            jsonTaskPropertiesResponse.addProperty(entry.getKey(), match.toString());
                        }
                    }
                    else
                    { // String literal value specified in profile.
                        jsonTaskPropertiesResponse.addProperty(entry.getKey(), entry.getValue().getAsString());
                    }
                }
                catch (InvalidPathException ipEx)
                { // This is to handle payload section for value "$."
                    jsonTaskPropertiesResponse.addProperty(entry.getKey(), payload);
                }
            }
            
            transformedTask =  json.fromJson(jsonTaskPropertiesResponse, TransformedTask.class);
        }
        else
        {
            throw new WebhookTransformationException("transform type is not supported.");
        }
        return transformedTask;
    }
}
