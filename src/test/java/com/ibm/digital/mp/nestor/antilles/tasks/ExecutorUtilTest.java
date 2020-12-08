package com.ibm.digital.mp.nestor.antilles.tasks;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonArray;

public class ExecutorUtilTest
{

    @Test
    public void testGenerateLuceneQuery()
    {
        Map<String, Object> queryParams = new HashMap<>();
        JsonArray operationIds = new JsonArray();
        operationIds.add("AddCustomer");
        operationIds.add("SyncSubscription");
        queryParams.put("payload.operationId", ExecutorUtil.constructCommaSeparatedValue(operationIds));
        queryParams.put("payload.essential", true);
        queryParams.put("createdDate", ExecutorUtil.createRange(0, 500123456L - 1));
        queryParams.put("status", ExecutorUtil.constructCommaSeparatedValue(ExecutorUtil.getIncompleteStatuses()));
        String luceneQuery = ExecutorUtil.generateLuceneQuery(queryParams);
        Assert.assertNotNull(luceneQuery);
    }
}
