package com.ibm.digital.mp.nestor.antilles.util;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.util.JsonUtilities;

public class DependentTaskUtilTest
{
    @Test
    public void testBuildTaskHierarchy_withMultiLayerDependency()
    {
        String data = "{\n" + "  \"_id\": \"1\",\n" + "  \"createdDate\": 1568593165175,\n" + "  \"status\": \"QUEUED\",\n"
                + "  \"referenceId\": \"A\",\n" + "  \"parentReferenceId\": \"B\",\n" + "  \"blockedTaskList\": [\n" + "    {\n"
                + "      \"_id\": \"2\",\n" + "      \"createdDate\": 1542329589237,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"B\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"3\",\n" + "      \"createdDate\": 1542329589238,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"4\",\n" + "      \"createdDate\": 1542329589239,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"B\",\n" + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"5\",\n" + "      \"createdDate\": 1542329589240,\n" + "      \"status\": \"BLOCKED\",\n"
                + "      \"referenceId\": \"C\"\n" + "    }\n" + "  ]\n" + "}";
        JsonObject taskPaylod = JsonUtilities.fromJson(data, JsonObject.class);
        JsonObject result = DependentTaskUtil.buildTaskHierarchy(taskPaylod);

        Assert.assertEquals("5", result.get("dependentTasks").getAsJsonArray().get(0).getAsJsonObject().get("id").getAsString());
    }

    @Test
    public void testBuildTaskHierarchy_withMultiLayerDependency_Deffered()
    {
        String data = "{\n" + "  \"_id\": \"1\",\n" + "  \"createdDate\": 1568593165175,\n" + "  \"status\": \"QUEUED\",\n"
                + "  \"referenceId\": \"A\",\n" + "  \"parentReferenceId\": \"B\",\n" + "  \"blockedTaskList\": [\n" + "    {\n"
                + "      \"_id\": \"2\",\n" + "      \"createdDate\": 1542329589237,\n" + "      \"status\": \"DEFERRED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"B\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"3\",\n" + "      \"createdDate\": 1542329589238,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"4\",\n" + "      \"createdDate\": 1542329589239,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"B\",\n" + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"5\",\n" + "      \"createdDate\": 1542329589240,\n" + "      \"status\": \"BLOCKED\",\n"
                + "      \"referenceId\": \"C\"\n" + "    }\n" + "  ]\n" + "}";
        JsonObject taskPaylod = JsonUtilities.fromJson(data, JsonObject.class);
        JsonObject result = DependentTaskUtil.buildTaskHierarchy(taskPaylod);
        Assert.assertEquals("5", result.get("dependentTasks").getAsJsonArray().get(0).getAsJsonObject().get("id").getAsString());
    }

    @Test
    public void testBuildTaskHierarchy_Completed_State()
    {
        String data = "{\n" + "  \"_id\": \"1\",\n" + "  \"createdDate\": 1568593165175,\n" + "  \"status\": \"COMPLETED\",\n"
                + "  \"referenceId\": \"A\",\n" + "  \"parentReferenceId\": \"B\",\n" + "  \"blockedTaskList\": [\n" + "    {\n"
                + "      \"_id\": \"2\",\n" + "      \"createdDate\": 1542329589237,\n" + "      \"status\": \"DEFERRED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"B\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"3\",\n" + "      \"createdDate\": 1542329589238,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"4\",\n" + "      \"createdDate\": 1542329589239,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"B\",\n" + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"5\",\n" + "      \"createdDate\": 1542329589240,\n" + "      \"status\": \"BLOCKED\",\n"
                + "      \"referenceId\": \"C\"\n" + "    }\n" + "  ]\n" + "}";
        JsonObject taskPaylod = JsonUtilities.fromJson(data, JsonObject.class);
        JsonObject result = DependentTaskUtil.buildTaskHierarchy(taskPaylod);
        Assert.assertTrue(result.get("dependentTasks").getAsJsonArray().size() == 0);
    }

    @Test
    public void testBuildTaskHierarchy_Blocked_referenceId()
    {
        String data = "{\n" + "  \"_id\": \"1\",\n" + "  \"createdDate\": 1568593165175,\n" + "  \"status\": \"QUEUED\",\n"
                + "  \"referenceId\": \"A\",\n" + "  \"parentReferenceId\": \"B\",\n" + "  \"blockedTaskList\": [\n" + "    {\n"
                + "      \"_id\": \"2\",\n" + "      \"createdDate\": 1542329589237,\n" + "      \"status\": \"NEW\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"B\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"3\",\n" + "      \"createdDate\": 1542329589238,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"4\",\n" + "      \"createdDate\": 1542329589239,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"B\",\n" + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"5\",\n" + "      \"createdDate\": 1542329589240,\n" + "      \"status\": \"BLOCKED\",\n"
                + "      \"referenceId\": \"C\"\n" + "    }\n" + "  ]\n" + "}";
        JsonObject taskPaylod = JsonUtilities.fromJson(data, JsonObject.class);
        JsonObject result = DependentTaskUtil.buildTaskHierarchy(taskPaylod);
        Assert.assertEquals("2", result.get("dependentTasks").getAsJsonArray().get(0).getAsJsonObject().get("id").getAsString());
    }

    @Test
    public void testBuildTaskHierarchy_Blocked_parentReferenceId()
    {
        String data = "{\n" + "  \"_id\": \"1\",\n" + "  \"createdDate\": 1568593165175,\n" + "  \"status\": \"QUEUED\",\n"
                + "  \"referenceId\": \"A\",\n" + "  \"parentReferenceId\": \"B\",\n" + "  \"blockedTaskList\": [\n" + "    {\n"
                + "      \"_id\": \"2\",\n" + "      \"createdDate\": 1542329589237,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\"\n" + "    },\n" + "    {\n" + "      \"_id\": \"3\",\n"
                + "      \"createdDate\": 1542329589238,\n" + "      \"status\": \"QUEUED\",\n" + "      \"referenceId\": \"A\",\n"
                + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n" + "      \"_id\": \"4\",\n"
                + "      \"createdDate\": 1542329589239,\n" + "      \"status\": \"QUEUED\",\n" + "      \"referenceId\": \"B\",\n"
                + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n" + "      \"_id\": \"5\",\n"
                + "      \"createdDate\": 1542329589240,\n" + "      \"status\": \"BLOCKED\",\n" + "      \"referenceId\": \"C\"\n"
                + "    }\n" + "  ]\n" + "}\n" + "";
        JsonObject taskPaylod = JsonUtilities.fromJson(data, JsonObject.class);
        JsonObject result = DependentTaskUtil.buildTaskHierarchy(taskPaylod);
        Assert.assertEquals("5", result.get("dependentTasks").getAsJsonArray().get(0).getAsJsonObject().get("id").getAsString());
    }

    @Test
    public void testBuildTaskHierarchy_Empty_Dependency()
    {
        String data = "{\n" + "  \"_id\": \"1\",\n" + "  \"createdDate\": 1568593165175,\n" + "  \"status\": \"QUEUED\",\n"
                + "  \"referenceId\": \"A\",\n" + "  \"parentReferenceId\": \"B\",\n" + "  \"blockedTaskList\": [\n" + "    \n" + "  ]\n"
                + "}\n" + "";
        JsonObject taskPaylod = JsonUtilities.fromJson(data, JsonObject.class);
        JsonObject result = DependentTaskUtil.buildTaskHierarchy(taskPaylod);
        Assert.assertTrue(result.get("dependentTasks").getAsJsonArray().size() == 0);
    }

    @Test
    public void testBuildTaskHierarchy_No_Dependency()
    {
        String data = "{\n" + "  \"_id\": \"1\",\n" + "  \"createdDate\": 1568593165175,\n" + "  \"status\": \"QUEUED\",\n"
                + "  \"referenceId\": \"A\",\n" + "  \"parentReferenceId\": \"B\"\n" + "}";
        JsonObject taskPaylod = JsonUtilities.fromJson(data, JsonObject.class);
        JsonObject result = DependentTaskUtil.buildTaskHierarchy(taskPaylod);
        Assert.assertTrue(result.get("dependentTasks").getAsJsonArray().size() == 0);
    }

    @Test
    public void testBuildTaskHierarchy_NoDate()
    {
        JsonObject result = DependentTaskUtil.buildTaskHierarchy(null);
        Assert.assertTrue(result == null);
    }

    @Test
    public void testBuildTaskHierarchy_missmatched_dependency()
    {
        String data = "{\n" + "  \"_id\": \"1\",\n" + "  \"createdDate\": 1568593165175,\n" + "  \"status\": \"QUEUED\",\n"
                + "  \"referenceId\": \"A\",\n" + "  \"parentReferenceId\": \"B\",\n" + "  \"blockedTaskList\": [\n" + "    {\n"
                + "      \"_id\": \"2\",\n" + "      \"createdDate\": 1542329589237,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"B\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"3\",\n" + "      \"createdDate\": 1542329589238,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"4\",\n" + "      \"createdDate\": 1542329589239,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"B\",\n" + "      \"parentReferenceId\": \"A\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"5\",\n" + "      \"createdDate\": 1542329589240,\n" + "      \"status\": \"BLOCKED\",\n"
                + "      \"referenceId\": \"C\"\n" + "    }\n" + "  ]\n" + "}\n" + "\n" + "";
        JsonObject taskPaylod = JsonUtilities.fromJson(data, JsonObject.class);
        JsonObject result = DependentTaskUtil.buildTaskHierarchy(taskPaylod);
        System.out.println(result.get("dependentTasks"));
        Assert.assertTrue(result.get("dependentTasks").getAsJsonArray().size() == 0);
    }

    @Test
    public void testBuildTaskHierarchy_duplicate_dependency()
    {
        String data = "{\n" + "  \"_id\": \"1\",\n" + "  \"createdDate\": 1568593165175,\n" + "  \"status\": \"QUEUED\",\n"
                + "  \"referenceId\": \"A\",\n" + "  \"parentReferenceId\": \"B\",\n" + "  \"blockedTaskList\": [\n" + "    {\n"
                + "      \"_id\": \"2\",\n" + "      \"createdDate\": 1542329589237,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"B\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"3\",\n" + "      \"createdDate\": 1542329589238,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"4\",\n" + "      \"createdDate\": 1542329589239,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"B\",\n" + "      \"parentReferenceId\": \"A\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"5\",\n" + "      \"createdDate\": 1542329589240,\n" + "      \"status\": \"BLOCKED\",\n"
                + "      \"referenceId\": \"C\"\n" + "    }\n" + "  ]\n" + "}\n" + "\n" + "";
        JsonObject taskPaylod = JsonUtilities.fromJson(data, JsonObject.class);
        JsonObject result = DependentTaskUtil.buildTaskHierarchy(taskPaylod);
        System.out.println(result.get("dependentTasks"));
        Assert.assertTrue(result.get("dependentTasks").getAsJsonArray().size() == 0);
    }

    @Test
    public void testBuildTaskHierarchy_oldestTaskCheck()
    {
        String data = "{\n" + "  \"_id\": \"1\",\n" + "  \"createdDate\": 1568593165175,\n" + "  \"status\": \"QUEUED\",\n"
                + "  \"referenceId\": \"A\",\n" + "  \"parentReferenceId\": \"B\",\n" + "  \"blockedTaskList\": [\n" + "    {\n"
                + "      \"_id\": \"2\",\n" + "      \"createdDate\": 1542329589237,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"B\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"3\",\n" + "      \"createdDate\": 1542329589238,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"C\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"20\",\n" + "      \"createdDate\": 1542329589236,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"A\",\n" + "      \"parentReferenceId\": \"B\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"4\",\n" + "      \"createdDate\": 1542329589239,\n" + "      \"status\": \"QUEUED\",\n"
                + "      \"referenceId\": \"B\",\n" + "      \"parentReferenceId\": \"A\"\n" + "    },\n" + "    {\n"
                + "      \"_id\": \"5\",\n" + "      \"createdDate\": 1542329589240,\n" + "      \"status\": \"BLOCKED\",\n"
                + "      \"referenceId\": \"C\"\n" + "    }\n" + "  ]\n" + "}\n" + "\n" + "";
        JsonObject taskPaylod = JsonUtilities.fromJson(data, JsonObject.class);
        JsonObject result = DependentTaskUtil.buildTaskHierarchy(taskPaylod);
        System.out.println(result.get("dependentTasks"));
        Assert.assertTrue(result.get("dependentTasks").getAsJsonArray().size() == 0);
    }

}
