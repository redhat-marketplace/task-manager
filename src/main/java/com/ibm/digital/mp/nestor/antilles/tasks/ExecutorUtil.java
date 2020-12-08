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

package com.ibm.digital.mp.nestor.antilles.tasks;

import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.ibm.digital.mp.nestor.tasks.Status;

public class ExecutorUtil
{
    ExecutorUtil()
    {

    }

    public static String createRange(long start, long end)
    {
        return "[" + start + " TO " + end + "]";
    }

    /**
     * Get Incomplete Statuses.
     * 
     * @return get json array of statuses
     */
    public static JsonArray getIncompleteStatuses()
    {
        JsonArray failedStatuses = new JsonArray();
        failedStatuses.add(Status.NEW.toString());
        failedStatuses.add(Status.RUNNING.toString());
        failedStatuses.add(Status.FAILED.toString());
        failedStatuses.add(Status.SCHEDULED.toString());
        failedStatuses.add(Status.BLOCKED.toString());
        failedStatuses.add(Status.NEEDS_ATTENTION.toString());
        failedStatuses.add(Status.POLLING.toString());
        failedStatuses.add(Status.QUEUED.toString());
        failedStatuses.add(Status.DEFERRED.toString());
        return failedStatuses;
    }

    /**
     * Construct comma separated values.
     * 
     * @param arrayOfString
     *            an array of strings
     * @return get string of values
     */
    public static String constructCommaSeparatedValue(JsonArray arrayOfString)
    {
        String commaSeparatedString = "";
        for (JsonElement stringValue : arrayOfString)
        {
            commaSeparatedString += stringValue + ",";
        }
        return commaSeparatedString.substring(0, commaSeparatedString.length() - 1);
    }

    /**
     * Get Lucene Query.
     * 
     * @param queryParams
     *            the query params
     * @return returns the lucene query
     */
    public static String generateLuceneQuery(Map<String, Object> queryParams)
    {
        String query = "";
        for (Entry<String, Object> queryParam : queryParams.entrySet())
        {
            Object valueObj = queryParam.getValue();
            if (!(valueObj instanceof Boolean))
            {
                String value = (String) valueObj;
                if (value.contains(","))
                {
                    value = constructOrQuery(value);
                }
                else if (!value.contains("TO"))
                {
                    value = "\"" + value + "\"";
                }
                valueObj = value;
            }
            query = query + queryParam.getKey() + ":" + valueObj + " AND ";
        }
        query = query.replaceAll("\"\"", "\"");
        return query.substring(0, query.length() - 4);
    }

    private static String constructOrQuery(String commaSeparatedValue)
    {
        String orQueryString = "(";
        for (String value : commaSeparatedValue.split(","))
        {
            orQueryString += value + " OR ";
        }
        return orQueryString.substring(0, orQueryString.length() - 4) + ")";
    }
}
