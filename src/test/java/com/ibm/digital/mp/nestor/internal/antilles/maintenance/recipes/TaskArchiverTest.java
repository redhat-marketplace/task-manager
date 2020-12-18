package com.ibm.digital.mp.nestor.internal.antilles.maintenance.recipes;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.digital.mp.nestor.db.Database;
import com.ibm.digital.mp.nestor.db.DbException;
import com.ibm.digital.mp.nestor.db.DbFactory;
import com.ibm.digital.mp.nestor.db.TaskArchiverDao;
import com.ibm.digital.mp.nestor.db.TaskArchiverDaoImpl;
import com.ibm.digital.mp.nestor.tasks.ExecutionContext;
import com.ibm.digital.mp.nestor.tasks.Profile;
import com.ibm.digital.mp.nestor.tasks.Result;
import com.ibm.digital.mp.nestor.tasks.TaskExecutionException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TaskArchiverDaoImpl.class, DbFactory.class })
@PowerMockIgnore("javax.management.*")
public class TaskArchiverTest
{
    TaskArchiver archiveTaskRecipe = new TaskArchiver();

    /**
     * initialized mock dependent objects.
     */
    @Before
    public void init() throws IllegalArgumentException, IllegalAccessException, DbException
    {
        // getDbMaintenance
        TaskArchiverDao dbMaintenance = PowerMock.createMock(TaskArchiverDaoImpl.class);
        PowerMock.mockStaticNice(DbFactory.class);
        EasyMock.expect(DbFactory.getTaskArchiverDao()).andReturn(dbMaintenance).anyTimes();

        Profile prof1 = new Profile();
        MemberModifier.field(Profile.class, "_id").set(prof1, "srm-dev");
        prof1.getConfiguration().addProperty("archivePolicyInDays", 120);

        Profile prof2 = new Profile();
        MemberModifier.field(Profile.class, "_id").set(prof2, "sa-ip");
        prof1.getConfiguration().addProperty("archivePolicyInDays", 120);

        List<Profile> profiles = new ArrayList<>();
        profiles.add(prof1);
        profiles.add(prof2);

        Database db = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(db).anyTimes();

        EasyMock.expect(db.fetchAllProfiles()).andReturn(profiles).anyTimes();

        List<JsonObject> data = new ArrayList<>();
        JsonObject dbRecords;
        for (int i = 1; i <= 5; i++)
        {
            dbRecords = new JsonObject();
            dbRecords.addProperty("_id", "123456789" + i);
            dbRecords.addProperty("modifiedDate", new Date().getTime() + i);

            data.add(dbRecords);
        }
        EasyMock.expect(dbMaintenance.fetchTasksToArchive(EasyMock.anyObject(JsonObject.class), EasyMock.anyObject(JsonObject.class),
                EasyMock.anyInt())).andReturn(data).anyTimes();

        dbMaintenance.updateRevision(EasyMock.anyObject(List.class));
        dbMaintenance.persistTasks(EasyMock.anyObject(List.class));
        EasyMock.expectLastCall().andVoid().anyTimes();

        EasyMock.expect(dbMaintenance.removeArchivedTasks(EasyMock.anyObject(JsonArray.class))).andReturn(true).anyTimes();

        EasyMock.expect(dbMaintenance.countRecordsCopied(EasyMock.anyString())).andReturn(2L).anyTimes();

        PowerMock.replayAll();
    }

    @Test
    public void testExecuteStartStep() throws TaskExecutionException
    {
        String data = "{defaultArchivePolicyInDays:120,onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, new JsonObject(), configuration, "START", 0, 10);
        Result result = archiveTaskRecipe.execute(executionContext);

        Assert.assertEquals("prepare", result.getNextStep());
    }

    @Test
    public void testExecutePrepareStep() throws TaskExecutionException
    {
        String data = "{defaultArchivePolicyInDays:120, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, new JsonObject(), configuration, "prepare", 0, 10);
        Result result = archiveTaskRecipe.execute(executionContext);

        Assert.assertEquals("findAndCopy:srm-dev:120", result.getNextStep());
    }

    @Test
    public void testExecuteFindAndCopyStep() throws TaskExecutionException
    {
        String data = "{defaultArchivePolicyInDays:120, onCandidArchivePolicyInDays:3,"
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, new JsonObject(), configuration, "prepare", 0, 10);
        Result result = archiveTaskRecipe.execute(executionContext);

        executionContext.setStepId(result.getNextStep());
        result = archiveTaskRecipe.execute(executionContext);
        Assert.assertEquals("validateData:srm-dev", result.getNextStep());
    }

    @Test
    public void testExecuteFindAndCopyStepWithWrongConfiguration()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 0);
        JsonArray owners = new JsonArray();
        owners.add("srm-dev");
        vault.add("purgedModules", owners);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:srm-dev:120");
        steps.add("validateData:srm-dev");
        steps.add("removeData:srm-dev");
        steps.add("findAndCopy:default:120");
        steps.add("validateData:default");
        steps.add("removeData:default");
        vault.add("steps", steps);

        String data = "{defaultArchivePolicyInDays:120, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:5001, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "findAndCopy:srm-dev:120", 0,
                10);
        try
        {
            archiveTaskRecipe.execute(executionContext);
            // expecting exception
            Assert.assertFalse(true);
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testExecuteWrongStep()
    {
        String data = "{defaultArchivePolicyInDays:120, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, new JsonObject(), configuration, "abcd", 0, 10);
        try
        {
            archiveTaskRecipe.execute(executionContext);
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testExecuteWrongPatternStep()
    {
        String data = "{defaultArchivePolicyInDays:120, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 0);
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "abcd:abcd:120", 0, 10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);
            Assert.assertNull(result);
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteFindAndCopyHasMoreStep()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 0);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:default:120");
        steps.add("validateData:default");
        steps.add("removeData:default");
        vault.add("steps", steps);
        vault.add("purgedModules", new JsonArray());
        String data = "{defaultArchivePolicyInDays:120, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:4, archiveInsertRecordsPerRuns : 4, "
                + "defaultTimeDelayinCopyAndFind:1,defaultTimeDelayBetweenCopyAndFind:1}";

        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "findAndCopy:srm-dev:120", 0,
                10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("validateData:default", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteValidateDataStep()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 1);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:srm-dev:120");
        steps.add("validateData:srm-dev");
        steps.add("removeData:srm-dev");
        vault.add("steps", steps);
        vault.add("purgedModules", new JsonArray());
        vault.addProperty("minModifiedDate", 123);
        vault.addProperty("maxModifiedDate", 456);
        vault.addProperty("fetchMore", false);

        JsonArray ids = new JsonArray();
        ids.add("1");
        ids.add("2");
        vault.add("archivedTaskIds", ids);

        String data = "{defaultArchivePolicyInDays:120, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:5, archiveInsertRecordsPerRuns : 2}";

        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "validateData:srm-dev", 0,
                10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("removeData:srm-dev", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteValidateDataDefaultStep()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 1);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:default:120");
        steps.add("validateData:default");
        steps.add("removeData:default");
        vault.add("steps", steps);
        JsonArray owners = new JsonArray();
        owners.add("srm-dev");
        owners.add("sa-api");
        vault.add("purgedModules", owners);
        vault.addProperty("minModifiedDate", 123);
        vault.addProperty("maxModifiedDate", 456);
        vault.addProperty("fetchMore", false);
        JsonArray ids = new JsonArray();
        ids.add("1");
        ids.add("2");
        vault.add("archivedTaskIds", ids);

        String data = "{defaultArchivePolicyInDays:120, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:5, archiveInsertRecordsPerRuns : 2}";

        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "validateData:default", 0,
                10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("removeData:default", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteValidateDataOnlyDefaultStep()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 1);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:default:120");
        steps.add("validateData:default");
        steps.add("removeData:default");
        vault.add("steps", steps);
        vault.add("purgedModules", new JsonArray());
        vault.addProperty("minModifiedDate", 123);
        vault.addProperty("maxModifiedDate", 456);
        vault.addProperty("fetchMore", false);
        JsonArray ids = new JsonArray();
        ids.add("1");
        ids.add("2");
        vault.add("archivedTaskIds", ids);

        String data = "{defaultArchivePolicyInDays:90, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:5, archiveInsertRecordsPerRuns : 2}";

        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "validateData:default", 0,
                10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("removeData:default", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteValidateDataValidationFailedStep() throws DbException
    {

        TaskArchiverDao dbMaintenance = PowerMock.createMock(TaskArchiverDaoImpl.class);
        PowerMock.mockStaticNice(DbFactory.class);
        EasyMock.expect(DbFactory.getTaskArchiverDao()).andReturn(dbMaintenance).anyTimes();
        EasyMock.expect(dbMaintenance.countRecordsCopied(EasyMock.anyString())).andReturn(1L).anyTimes();
        PowerMock.replayAll();

        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 1);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:srm-dev:120");
        steps.add("validateData:srm-dev");
        steps.add("removeData:srm-dev");
        vault.add("steps", steps);
        vault.addProperty("minModifiedDate", 123);
        vault.addProperty("maxModifiedDate", 456);
        vault.addProperty("fetchMore", false);
        vault.add("purgedModules", new JsonArray());
        JsonArray ids = new JsonArray();
        ids.add("1");
        ids.add("2");
        vault.add("archivedTaskIds", ids);

        String data = "{defaultArchivePolicyInDays:90, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:5, archiveInsertRecordsPerRuns : 2}";

        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "validateData:srm-dev", 0,
                10);
        try
        {
            archiveTaskRecipe.execute(executionContext);
            // expecting error hence below line should not execute
            Assert.assertTrue(false);
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testExecuteValidateSkipStepForMin() throws DbException
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 1);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:srm-dev:120");
        steps.add("validateData:srm-dev");
        steps.add("removeData:srm-dev");
        vault.add("steps", steps);
        vault.add("purgedModules", new JsonArray());
        JsonArray ids = new JsonArray();
        ids.add("1");
        ids.add("2");
        vault.add("archivedTaskIds", ids);

        String data = "{defaultArchivePolicyInDays:90, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:5, archiveInsertRecordsPerRuns : 2}";

        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "validateData:srm-dev", 0,
                10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);
            Assert.assertEquals("removeData:srm-dev", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteValidateSkipStepForMax() throws DbException
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 1);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:srm-dev:120");
        steps.add("validateData:srm-dev");
        steps.add("removeData:srm-dev");
        vault.add("steps", steps);
        vault.addProperty("minModifiedDate", 123);
        vault.add("purgedModules", new JsonArray());
        JsonArray ids = new JsonArray();
        ids.add("1");
        ids.add("2");
        vault.add("archivedTaskIds", ids);

        String data = "{defaultArchivePolicyInDays:90, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:5, archiveInsertRecordsPerRuns : 2}";

        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "validateData:srm-dev", 0,
                10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);
            Assert.assertEquals("removeData:srm-dev", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteRemoveDataModuleStep()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 2);
        vault.add("purgedModules", new JsonArray());
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:srm-dev:120");
        steps.add("validateData:srm-dev");
        steps.add("removeData:srm-dev");
        steps.add("findAndCopy:sa-api:120");
        steps.add("validateData:sa-api");
        steps.add("removeData:sa-api");
        vault.add("steps", steps);
        vault.addProperty("minModifiedDate", 123);
        vault.addProperty("maxModifiedDate", 456);
        vault.addProperty("fetchMore", false);
        JsonArray ids = new JsonArray();
        ids.add("1");
        ids.add("2");
        vault.add("archivedTaskIds", ids);

        String data = "{defaultArchivePolicyInDays:90, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "removeData:srm-dev", 0, 10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("findAndCopy:sa-api:120", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteRemoveDataDefaultStep()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 8);
        vault.add("purgedModules", new JsonArray());
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:srm-dev:120");
        steps.add("validateData:srm-dev");
        steps.add("removeData:srm-dev");
        steps.add("findAndCopy:sa-api:120");
        steps.add("validateData:sa-api");
        steps.add("removeData:sa-api");
        steps.add("findAndCopy:default:120");
        steps.add("validateData:default");
        steps.add("removeData:default");
        steps.add("findAndCopy:onCandid:120");
        steps.add("validateData:onCandid");
        steps.add("removeData:onCandid");
        vault.add("steps", steps);
        vault.addProperty("fetchMore", false);
        JsonArray ids = new JsonArray();
        ids.add("1");
        ids.add("2");
        vault.add("archivedTaskIds", ids);

        String data = "{defaultArchivePolicyInDays:90, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "removeData:default", 0, 10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("findAndCopy:onCandid:120", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteRemoveDataDefaultNinStep()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 1);
        JsonArray owners = new JsonArray();
        owners.add("srm-dev");
        owners.add("sa-api");
        vault.add("purgedModules", owners);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:srm-dev:120");
        steps.add("validateData:srm-dev");
        steps.add("removeData:srm-dev");
        steps.add("findAndCopy:sa-api:120");
        steps.add("validateData:sa-api");
        steps.add("removeData:sa-api");
        vault.add("steps", steps);
        vault.addProperty("minModifiedDate", 123);
        vault.addProperty("maxModifiedDate", 456);
        vault.addProperty("fetchMore", false);
        JsonArray ids = new JsonArray();
        ids.add("1");
        ids.add("2");
        vault.add("archivedTaskIds", ids);

        String data = "{defaultArchivePolicyInDays:90, onCandidArchivePolicyInDays:3,"
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "removeData:default", 0, 10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("removeData:srm-dev", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteRemoveFetchMore()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 2);
        vault.add("purgedModules", new JsonArray());
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:srm-dev:120");
        steps.add("validateData:srm-dev");
        steps.add("removeData:srm-dev");
        steps.add("findAndCopy:sa-api:120");
        steps.add("validateData:sa-api");
        steps.add("removeData:sa-api");
        vault.add("steps", steps);
        vault.addProperty("minModifiedDate", 123);
        vault.addProperty("maxModifiedDate", 456);
        vault.addProperty("fetchMore", true);
        vault.addProperty("fallbackStepIndex", 0);
        JsonArray ids = new JsonArray();
        ids.add("1");
        ids.add("2");
        vault.add("archivedTaskIds", ids);

        String data = "{defaultArchivePolicyInDays:90,onCandidArchivePolicyInDays:3,"
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "removeData:srm-dev", 0, 10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("findAndCopy:srm-dev:120", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteRemoveFetchMoreWithDelayCount()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 2);
        vault.add("purgedModules", new JsonArray());
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:srm-dev:120");
        steps.add("validateData:srm-dev");
        steps.add("removeData:srm-dev");
        steps.add("findAndCopy:sa-api:120");
        steps.add("validateData:sa-api");
        steps.add("removeData:sa-api");
        vault.add("steps", steps);
        vault.addProperty("minModifiedDate", 123);
        vault.addProperty("maxModifiedDate", 456);
        vault.addProperty("fetchMore", true);
        vault.addProperty("fallbackStepIndex", 0);
        JsonArray ids = new JsonArray();
        ids.add("1");
        ids.add("2");
        vault.add("archivedTaskIds", ids);

        String data = "{defaultArchivePolicyInDays:90,onCandidArchivePolicyInDays:3,"
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200, " + "defaultTimeDelayBetweenCopyAndFind: 1}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "removeData:srm-dev", 0, 10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("findAndCopy:srm-dev:120", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteNullStep()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 1);
        vault.add("purgedModules", new JsonArray());
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:srm-dev:120");
        steps.add("validateData:srm-dev:120");
        steps.add("removeData:srm-dev");
        steps.add("findAndCopy:sa-api:120");
        steps.add("validateData:sa-api");
        steps.add("removeData:sa-api");
        vault.add("steps", steps);

        String data = "{defaultArchivePolicyInDays:90,onCandidArchivePolicyInDays:3,"
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "", 0, 10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertNull(result);
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteOnlyDefaultSetup() throws IllegalArgumentException, IllegalAccessException, DbException
    {
        // getDbMaintenance
        TaskArchiverDao dbMaintenance = PowerMock.createMock(TaskArchiverDaoImpl.class);
        PowerMock.mockStaticNice(DbFactory.class);
        Database db = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getTaskArchiverDao()).andReturn(dbMaintenance).anyTimes();
        EasyMock.expect(DbFactory.getDatabase()).andReturn(db).anyTimes();

        EasyMock.expect(db.fetchAllProfiles()).andReturn(new ArrayList<>()).anyTimes();

        List<JsonObject> data = new ArrayList<>();
        JsonObject dbRecords;
        for (int i = 1; i <= 5; i++)
        {
            dbRecords = new JsonObject();
            dbRecords.addProperty("_id", "123456789" + i);
            dbRecords.addProperty("modifiedDate", new Date().getTime() + i);

            data.add(dbRecords);
        }
        EasyMock.expect(dbMaintenance.fetchTasksToArchive(EasyMock.anyObject(JsonObject.class), EasyMock.anyObject(JsonObject.class),
                EasyMock.anyInt())).andReturn(data).anyTimes();

        dbMaintenance.persistTasks(EasyMock.anyObject(List.class));
        EasyMock.expectLastCall().andVoid();

        EasyMock.expect(dbMaintenance.removeArchivedTasks(EasyMock.anyObject(JsonArray.class))).andReturn(true).anyTimes();

        PowerMock.replayAll();

        String configData = "{defaultArchivePolicyInDays:90,onCandidArchivePolicyInDays:3,"
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(configData).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, new JsonObject(), configuration, "prepare", 0, 10);
        Result result;
        try
        {
            result = archiveTaskRecipe.execute(executionContext);
            Assert.assertEquals("findAndCopy:default:90", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteFindAndCopyOnlyDefaultSetup() throws IllegalArgumentException, IllegalAccessException, DbException
    {
        // getDbMaintenance
        TaskArchiverDao dbMaintenance = PowerMock.createMock(TaskArchiverDaoImpl.class);
        PowerMock.mockStaticNice(DbFactory.class);
        EasyMock.expect(DbFactory.getTaskArchiverDao()).andReturn(dbMaintenance).anyTimes();
        Database db = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(db).anyTimes();

        EasyMock.expect(db.fetchAllProfiles()).andReturn(new ArrayList<>()).anyTimes();

        List<JsonObject> data = new ArrayList<>();
        JsonObject dbRecords;
        for (int i = 1; i <= 5; i++)
        {
            dbRecords = new JsonObject();
            dbRecords.addProperty("_id", "123456789" + i);
            dbRecords.addProperty("modifiedDate", new Date().getTime() + i);

            data.add(dbRecords);
        }
        EasyMock.expect(dbMaintenance.fetchTasksToArchive(EasyMock.anyObject(JsonObject.class), EasyMock.anyObject(JsonObject.class),
                EasyMock.anyInt())).andReturn(data).anyTimes();
        dbMaintenance.updateRevision(EasyMock.anyObject(List.class));
        dbMaintenance.persistTasks(EasyMock.anyObject(List.class));
        EasyMock.expectLastCall().andVoid();

        EasyMock.expect(dbMaintenance.removeArchivedTasks(EasyMock.anyObject(JsonArray.class))).andReturn(true).anyTimes();

        PowerMock.replayAll();

        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 0);
        vault.add("purgedModules", new JsonArray());
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:default:120");
        steps.add("removeData:default");
        vault.add("steps", steps);
        String configData = "{defaultArchivePolicyInDays:90,onCandidArchivePolicyInDays:3,"
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns :200}";
        JsonObject configuration = new JsonParser().parse(configData).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "findAndCopy:default:90", 0,
                10);
        Result result;
        try
        {
            result = archiveTaskRecipe.execute(executionContext);
            Assert.assertEquals("removeData:default", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteFindAndCopyHasMoreStepWithNoDefaultTimeDelay()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 0);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:default:120");
        steps.add("validateData:default");
        steps.add("removeData:default");
        vault.add("steps", steps);
        vault.add("purgedModules", new JsonArray());
        String data = "{defaultArchivePolicyInDays:90,onCandidArchivePolicyInDays:3,"
                + "archiveFetchRecordsPerRuns:4, archiveInsertRecordsPerRuns :4}";

        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "findAndCopy:srm-dev:120", 0,
                10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("validateData:default", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteProfileWithEmptyArchiveData() throws IllegalArgumentException, IllegalAccessException, DbException
    {
        // getDbMaintenance
        TaskArchiverDao dbMaintenance = PowerMock.createMock(TaskArchiverDaoImpl.class);
        PowerMock.mockStaticNice(DbFactory.class);
        EasyMock.expect(DbFactory.getTaskArchiverDao()).andReturn(dbMaintenance).anyTimes();

        List<JsonObject> data = new ArrayList<>();
        EasyMock.expect(dbMaintenance.fetchTasksToArchive(EasyMock.anyObject(JsonObject.class), EasyMock.anyObject(JsonObject.class),
                EasyMock.anyInt())).andReturn(data).anyTimes();

        dbMaintenance.persistTasks(EasyMock.anyObject(List.class));
        EasyMock.expectLastCall().andVoid();

        EasyMock.expect(dbMaintenance.removeArchivedTasks(EasyMock.anyObject(JsonArray.class))).andReturn(true).anyTimes();

        PowerMock.replayAll();

        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 0);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:default:0");
        steps.add("validateData:default");
        steps.add("removeData:default");
        vault.add("steps", steps);
        vault.add("purgedModules", new JsonArray());
        String configData = "{defaultArchivePolicyInDays:90,onCandidArchivePolicyInDays:3,"
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns :200}";
        JsonObject configuration = new JsonParser().parse(configData).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "findAndCopy:srm-dev:0", 0,
                0);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("validateData:default", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteFindAndCopyStepWithoutBulk()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 0);
        JsonArray owners = new JsonArray();
        owners.add("srm-dev");
        vault.add("purgedModules", owners);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:srm-dev:120");
        steps.add("validateData:srm-dev");
        steps.add("removeData:srm-dev");
        steps.add("findAndCopy:default:120");
        steps.add("validateData:default");
        steps.add("removeData:default");
        vault.add("steps", steps);

        String data = "{defaultArchivePolicyInDays:120, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:1000, archiveInsertRecordsPerRuns : 200,  disableBulkUpsert: true}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "findAndCopy:srm-dev:120", 0,
                10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("validateData:srm-dev", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteFindAndCopyForOnCandid()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 3);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:default:120");
        steps.add("validateData:default");
        steps.add("removeData:default");
        steps.add("findAndCopy:onCandid:120");
        steps.add("validateData:onCandid");
        steps.add("removeData:onCandid");
        vault.add("steps", steps);
        vault.add("purgedModules", new JsonArray());
        String data = "{defaultArchivePolicyInDays:120, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:4, archiveInsertRecordsPerRuns : 4, "
                + "defaultTimeDelayinCopyAndFind:1,defaultTimeDelayBetweenCopyAndFind:1}";

        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "findAndCopy:onCandid:120",
                0, 10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("validateData:onCandid", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteValidateDataStepForOnCandid()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 4);
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:default:120");
        steps.add("validateData:default");
        steps.add("removeData:default");
        steps.add("findAndCopy:onCandid:120");
        steps.add("validateData:onCandid");
        steps.add("removeData:onCandid");
        vault.add("steps", steps);
        vault.add("purgedModules", new JsonArray());
        vault.addProperty("minModifiedDate", 123);
        vault.addProperty("maxModifiedDate", 456);
        vault.addProperty("fetchMore", false);
        JsonArray ids = new JsonArray();
        ids.add("1");
        ids.add("2");
        vault.add("archivedTaskIds", ids);

        String data = "{defaultArchivePolicyInDays:120, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:5, archiveInsertRecordsPerRuns : 2}";

        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "validateData:onCandid", 0,
                10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("removeData:onCandid", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteRemoveDataModuleStepForOnCandid()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 5);
        vault.add("purgedModules", new JsonArray());
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:default:120");
        steps.add("validateData:default");
        steps.add("removeData:default");
        steps.add("findAndCopy:onCandid:120");
        steps.add("validateData:onCandid");
        steps.add("removeData:onCandid");
        steps.add("findAndDelete");
        vault.add("steps", steps);
        vault.addProperty("minModifiedDate", 123);
        vault.addProperty("maxModifiedDate", 456);
        vault.addProperty("fetchMore", false);

        String data = "{defaultArchivePolicyInDays:90, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "removeData:onCandid", 0,
                10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("findAndDelete", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteMarkForDeletion() throws DbException
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 5);
        vault.add("purgedModules", new JsonArray());
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:default:120");
        steps.add("validateData:default");
        steps.add("removeData:default");
        steps.add("findAndCopy:onCandid:120");
        steps.add("validateData:onCandid");
        steps.add("removeData:onCandid");
        steps.add("findAndDelete");
        vault.add("steps", steps);
        vault.addProperty("minModifiedDate", 123);
        vault.addProperty("maxModifiedDate", 456);
        vault.addProperty("fetchMore", false);

        TaskArchiverDao dbMaintenance = PowerMock.createMock(TaskArchiverDaoImpl.class);
        PowerMock.mockStaticNice(DbFactory.class);
        EasyMock.expect(DbFactory.getTaskArchiverDao()).andReturn(dbMaintenance).anyTimes();

        EasyMock.expect(dbMaintenance.deleteTasks(EasyMock.anyObject(JsonObject.class))).andReturn(20L).anyTimes();

        PowerMock.replayAll();
        String data = "{defaultArchivePolicyInDays:90, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "findAndDelete", 0, 10);
        try
        {
            Result result = archiveTaskRecipe.execute(executionContext);

            Assert.assertEquals("START", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecuteMarkForDeletionWithError() throws DbException
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 5);
        vault.add("purgedModules", new JsonArray());
        JsonArray steps = new JsonArray();
        steps.add("findAndCopy:default:120");
        steps.add("validateData:default");
        steps.add("removeData:default");
        steps.add("findAndCopy:onCandid:120");
        steps.add("validateData:onCandid");
        steps.add("removeData:onCandid");
        steps.add("findAndDelete");
        vault.add("steps", steps);
        vault.addProperty("minModifiedDate", 123);
        vault.addProperty("maxModifiedDate", 456);
        vault.addProperty("fetchMore", false);

        TaskArchiverDao dbMaintenance = PowerMock.createMock(TaskArchiverDaoImpl.class);
        PowerMock.mockStaticNice(DbFactory.class);
        EasyMock.expect(DbFactory.getTaskArchiverDao()).andReturn(dbMaintenance).anyTimes();

        EasyMock.expect(dbMaintenance.deleteTasks(EasyMock.anyObject(JsonObject.class)))
                .andThrow(new RuntimeException("some DB exception...")).anyTimes();

        PowerMock.replayAll();
        String data = "{defaultArchivePolicyInDays:90, onCandidArchivePolicyInDays:3, "
                + "archiveFetchRecordsPerRuns:500, archiveInsertRecordsPerRuns : 200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "findAndDelete", 0, 10);
        try
        {
            archiveTaskRecipe.execute(executionContext);
            // expecting error
            Assert.assertTrue(false);
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(true);
        }
    }
}
