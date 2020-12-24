package com.ibm.digital.mp.nestor.internal.antilles.maintenance.recipes;

import java.util.ArrayList;
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
public class TaskPurgerTest
{
    TaskPurger taskPurgerRecipe = new TaskPurger();

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
        prof1.getConfiguration().addProperty("purgePolicyInDays", 120);

        Profile prof2 = new Profile();
        MemberModifier.field(Profile.class, "_id").set(prof2, "sa-ip");
        prof2.getConfiguration().addProperty("purgePolicyInDays", 120);

        List<Profile> profiles = new ArrayList<>();
        profiles.add(prof1);
        profiles.add(prof2);
        Database db = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(db).anyTimes();
        
        EasyMock.expect(db.fetchAllProfiles()).andReturn(profiles).anyTimes();

        List<JsonObject> data = new ArrayList<>();
        data.add(new JsonObject());
        data.add(new JsonObject());
        data.add(new JsonObject());
        data.add(new JsonObject());
        EasyMock.expect(dbMaintenance.fetchTasksToPurge(EasyMock.anyString(), EasyMock.anyInt())).andReturn(data).anyTimes();

        EasyMock.expect(dbMaintenance.purgeTask(EasyMock.anyObject(List.class))).andReturn(true).anyTimes();

        PowerMock.replayAll();

    }

    @Test
    public void testExecute_Start_step() throws TaskExecutionException
    {
        String data = "{defaultPurgePolicyInDays:90,purgeRecordsPerRuns:200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, new JsonObject(), configuration, "START", 0, 10);
        Result result = taskPurgerRecipe.execute(executionContext);

        Assert.assertEquals("prepare", result.getNextStep());
    }

    @Test
    public void testExecute_Prepare_step() throws TaskExecutionException
    {
        String data = "{defaultPurgePolicyInDays:90,purgeRecordsPerRuns:200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, new JsonObject(), configuration, "prepare", 0, 10);
        Result result = taskPurgerRecipe.execute(executionContext);

        Assert.assertEquals("fetchAndPurge:srm-dev:120", result.getNextStep());
    }

    @Test
    public void testExecute_FetchAndCopy_step() throws TaskExecutionException, DbException
    {

        String data = "{defaultPurgePolicyInDays:90,purgeRecordsPerRuns:200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, new JsonObject(), configuration, "prepare", 0, 10);
        Result result = taskPurgerRecipe.execute(executionContext);
        executionContext.setStepId(result.getNextStep());

        // getDbMaintenance
        TaskArchiverDao dbMaintenance = PowerMock.createMock(TaskArchiverDaoImpl.class);
        PowerMock.mockStaticNice(DbFactory.class);
        EasyMock.expect(DbFactory.getTaskArchiverDao()).andReturn(dbMaintenance).anyTimes();
        EasyMock.expect(dbMaintenance.fetchTasksToPurge(EasyMock.anyString(), EasyMock.anyInt())).andReturn(new ArrayList<>()).anyTimes();
        EasyMock.expect(dbMaintenance.purgeTask(EasyMock.anyObject(List.class))).andReturn(true).anyTimes();

        PowerMock.replayAll();

        result = taskPurgerRecipe.execute(executionContext);
        Assert.assertEquals("fetchAndPurge:sa-ip:120", result.getNextStep());
    }

    @Test
    public void testExecute_wrong_step()
    {
        String data = "{defaultPurgePolicyInDays:90,purgeRecordsPerRuns:200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, new JsonObject(), configuration, "abcd", 0, 10);
        try
        {
            taskPurgerRecipe.execute(executionContext);
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testExecute_wrong_pattern_step()
    {
        String data = "{defaultPurgePolicyInDays:90,purgeRecordsPerRuns:200}";
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 0);
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();
        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "abcd:abcd:120", 0, 10);
        try
        {
            Result result = taskPurgerRecipe.execute(executionContext);
            Assert.assertNull(result);
        }
        catch (TaskExecutionException ex)
        {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testExecute_FetchAndPurge__hasMore_step()
    {
        String data = "{defaultPurgePolicyInDays:90,purgeRecordsPerRuns:200,defaultTimeDelayBetweenFetchAndPurge:100}";
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 0);
        vault.addProperty("skipRecords", 0);
        vault.add("purgedModules", new JsonArray());

        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "fetchAndPurge:srm-dev:120",
                0, 10);
        try
        {
            Result result = taskPurgerRecipe.execute(executionContext);

            Assert.assertEquals("fetchAndPurge:srm-dev:120", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecute_FetchAndPurge__hasMore_step_with_nodefaultTimeDelay()
    {
        String data = "{defaultPurgePolicyInDays:90,purgeRecordsPerRuns:200}";
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 0);
        vault.addProperty("skipRecords", 0);
        vault.add("purgedModules", new JsonArray());

        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "fetchAndPurge:srm-dev:120",
                0, 10);
        try
        {
            Result result = taskPurgerRecipe.execute(executionContext);

            Assert.assertEquals("fetchAndPurge:srm-dev:120", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecute_purgeData_lastprofile_step() throws DbException, IllegalAccessException
    {

        // getDbMaintenance
        TaskArchiverDao dbMaintenance = PowerMock.createMock(TaskArchiverDaoImpl.class);
        PowerMock.mockStaticNice(DbFactory.class);
        EasyMock.expect(DbFactory.getTaskArchiverDao()).andReturn(dbMaintenance).anyTimes();

        Profile prof1 = new Profile();
        MemberModifier.field(Profile.class, "_id").set(prof1, "srm-dev");
        prof1.getConfiguration().addProperty("purgePolicyInDays", 120);

        Profile prof2 = new Profile();
        MemberModifier.field(Profile.class, "_id").set(prof2, "sa-ip");
        prof2.getConfiguration().addProperty("purgePolicyInDays", 120);

        List<Profile> profiles = new ArrayList<>();
        profiles.add(prof1);
        profiles.add(prof2);
        Database db = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(db).anyTimes();
        EasyMock.expect(db.fetchAllProfiles()).andReturn(profiles).anyTimes();
        EasyMock.expect(dbMaintenance.fetchTasksToPurge(EasyMock.anyString(), EasyMock.anyInt())).andReturn(new ArrayList<>()).anyTimes();
        EasyMock.expect(dbMaintenance.purgeTask(EasyMock.anyObject(List.class))).andReturn(true).anyTimes();

        PowerMock.replayAll();

        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 0);
        vault.addProperty("skipRecords", 0);
        JsonArray purgedModules = new JsonArray();
        purgedModules.add("default");
        vault.add("purgedModules", purgedModules);
        JsonArray modules = new JsonArray();
        modules.add("fetchAndPurge:srm-dev:120");
        modules.add("fetchAndPurge:sa-api:120");
        vault.add("steps", modules);

        String data = "{defaultPurgePolicyInDays:90,purgeRecordsPerRuns:200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "fetchAndPurge:default:120",
                0, 10);
        try
        {
            Result result = taskPurgerRecipe.execute(executionContext);

            Assert.assertEquals("START", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecute_FetchAndPurgeData_default_Nin_step() throws DbException
    {

        // getDbMaintenance
        TaskArchiverDao dbMaintenance = PowerMock.createMock(TaskArchiverDaoImpl.class);
        PowerMock.mockStaticNice(DbFactory.class);
        EasyMock.expect(DbFactory.getTaskArchiverDao()).andReturn(dbMaintenance).anyTimes();
        EasyMock.expect(dbMaintenance.fetchTasksToPurge(EasyMock.anyString(), EasyMock.anyInt())).andReturn(new ArrayList<>()).anyTimes();
        EasyMock.expect(dbMaintenance.purgeTask(EasyMock.anyObject(List.class))).andReturn(true).anyTimes();

        PowerMock.replayAll(TaskArchiverDao.class);
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 1);
        vault.addProperty("skipRecords", 0);
        JsonArray owners = new JsonArray();
        owners.add("srm-dev");
        owners.add("sa-api");
        vault.add("purgedModules", owners);
        JsonArray modules = new JsonArray();
        modules.add("fetchAndPurge:srm-dev:120");
        modules.add("fetchAndPurge:sa-api:120");
        vault.add("steps", modules);

        String data = "{defaultPurgePolicyInDays:90,purgeRecordsPerRuns:200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "fetchAndPurge:default:120",
                0, 10);
        try
        {
            Result result = taskPurgerRecipe.execute(executionContext);

            Assert.assertEquals("START", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testExecute_null_step()
    {
        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 1);
        vault.addProperty("skipRecords", 0);
        vault.add("purgedModules", new JsonArray());
        JsonArray modules = new JsonArray();
        modules.add("fetchAndPurgey:srm-dev:120");
        modules.add("fetchAndPurge:sa-api:120");
        vault.add("steps", modules);

        String data = "{defaultPurgePolicyInDays:90,purgeRecordsPerRuns:200}";
        JsonObject configuration = new JsonParser().parse(data).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "", 0, 10);
        try
        {
            Result result = taskPurgerRecipe.execute(executionContext);

            Assert.assertNull(result);
        }
        catch (TaskExecutionException ex)
        {
            ex.printStackTrace();
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testExecute_findAndCopy_Only_default_setup() throws IllegalArgumentException, DbException
    {
        // getDbMaintenance
        TaskArchiverDao dbMaintenance = PowerMock.createMock(TaskArchiverDaoImpl.class);
        PowerMock.mockStaticNice(DbFactory.class);
        EasyMock.expect(DbFactory.getTaskArchiverDao()).andReturn(dbMaintenance).anyTimes();
        Database db = PowerMock.createMock(Database.class);
        EasyMock.expect(DbFactory.getDatabase()).andReturn(db).anyTimes();
        EasyMock.expect(db.fetchAllProfiles()).andReturn(new ArrayList<>()).anyTimes();

        List<JsonObject> data = new ArrayList<>();
        data.add(new JsonObject());
        data.add(new JsonObject());
        data.add(new JsonObject());
        data.add(new JsonObject());
        EasyMock.expect(dbMaintenance.fetchTasksToPurge(EasyMock.anyString(), EasyMock.anyInt())).andReturn(data).anyTimes();

        EasyMock.expect(dbMaintenance.purgeTask(EasyMock.anyObject(List.class))).andReturn(true).anyTimes();

        PowerMock.replayAll();

        JsonObject vault = new JsonObject();
        vault.addProperty("stepIndex", 0);
        vault.addProperty("skipRecords", 0);
        vault.add("purgedModules", new JsonArray());
        JsonArray modules = new JsonArray();
        modules.add("findAndCopy:default:120");
        modules.add("removeData:default:120");
        vault.add("steps", modules);
        String configData = "{defaultPurgePolicyInDays:90,purgeRecordsPerRuns:200}";
        JsonObject configuration = new JsonParser().parse(configData).getAsJsonObject();

        ExecutionContext executionContext = new ExecutionContext(null, null, null, null, vault, configuration, "fetchAndPurge:default:90",
                0, 10);
        Result result;
        try
        {
            result = taskPurgerRecipe.execute(executionContext);
            Assert.assertEquals("fetchAndPurge:default:90", result.getNextStep());
        }
        catch (TaskExecutionException ex)
        {
            // TODO Auto-generated catch block
            ex.printStackTrace();
            Assert.assertTrue(false);
        }
    }

}
