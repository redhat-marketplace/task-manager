package com.ibm.digital.mp.nestor.antilles.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.digital.mp.nestor.tasks.Status;
import com.ibm.digital.mp.nestor.util.JsonUtilities;

public class DependentTaskUtil
{
    private DependentTaskUtil()
    {
    }

    /**
     * build task hierarchy based on the dependent tasks.
     * 
     * @param taskPaylod
     *            - task with dependent tasks.
     * @return task with hierarchy.
     */
    public static JsonObject buildTaskHierarchy(JsonObject taskPaylod)
    {

        if (taskPaylod == null)
        {
            return null;
        }
        
        SubTask task = getTaskObject(taskPaylod);

        // if the task itself in BLOCKED or NA state
        if (isBlockingTask(task.getStatus()))
        {
            return JsonUtilities.toJsonObject(task);
        }

        Map<String, SubTask> taskRefMap = convertDependentTasktoMap(taskPaylod);

        Set<String> taskPath = new LinkedHashSet<>();

        SubTask blockingTask = getDependentTask(task, taskRefMap, taskPath);

        if (blockingTask != null)
        {
            task.addDependentTask(blockingTask);
        }
        return JsonUtilities.toJsonObject(task);
    }

    private static SubTask getDependentTask(SubTask currentTask, Map<String, SubTask> taskRefMap, Set<String> taskPath)
    {
        // Safety check
        if (currentTask == null || !taskPath.add(currentTask.getId()))
        {
            return null;
        }

        // get blocking task via referenceId
        SubTask tempTask = taskRefMap.get(currentTask.getReferenceId());

        if (tempTask != null)
        {
            if (isBlockingTask(tempTask.getStatus()))
            {
                return tempTask;
            }
            else
            {
                tempTask = getDependentTask(tempTask, taskRefMap, taskPath);
                if (tempTask != null)
                {
                    return tempTask;
                }
            }
        }

        // get blocking task via parent referenceId
        tempTask = taskRefMap.get(currentTask.getParentReferenceId());

        if (tempTask != null)
        {
            if (isBlockingTask(tempTask.getStatus()))
            {
                return tempTask;
            }
            else
            {
                tempTask = getDependentTask(tempTask, taskRefMap, taskPath);
            }
        }

        return tempTask;
    }

    private static boolean isBlockingTask(String status)
    {
        return !Status.QUEUED.toString().equals(status) && !Status.DEFERRED.toString().equals(status);
    }

    /**
     * Logic to build a map with RefernenceId and the oldest task.
     * 
     * @param taskPaylod
     *            - data from DB.
     * @return map with referenceId and task.
     */
    private static Map<String, SubTask> convertDependentTasktoMap(JsonObject taskPaylod)
    {
        Map<String, SubTask> taskRefMap = new HashMap<>();
        SubTask dependentTask;
        SubTask tempTask;
        if (taskPaylod.get("blockedTaskList") != null)
        {
            for (JsonElement taskObj : taskPaylod.get("blockedTaskList").getAsJsonArray())
            {
                dependentTask = getTaskObject(taskObj.getAsJsonObject());
                tempTask = taskRefMap.get(dependentTask.getReferenceId());
                if (tempTask == null)
                {
                    taskRefMap.put(dependentTask.getReferenceId(), dependentTask);
                }
                else
                {
                    // Map to have oldest task per referenceId
                    if (dependentTask.getCreatedDate().compareTo(tempTask.getCreatedDate()) < 0)
                    {
                        taskRefMap.put(dependentTask.getReferenceId(), dependentTask);
                    }
                }
            }
        }

        return taskRefMap;
    }

    private static SubTask getTaskObject(JsonObject obj)
    {
        SubTask task = new SubTask();

        task.setId(obj.get("_id").getAsString());
        task.setCreatedDate(obj.get("createdDate").getAsLong());
        task.setStatus(obj.get("status").getAsString());
        if (obj.get("referenceId") != null)
        {
            task.setReferenceId(obj.get("referenceId").getAsString());
        }

        if (obj.get("parentReferenceId") != null)
        {
            task.setParentReferenceId(obj.get("parentReferenceId").getAsString());
        }

        return task;
    }

    private static class SubTask
    {
        private String id;
        private Long createdDate;
        private String referenceId;
        private String parentReferenceId;
        private String status;
        private Set<SubTask> dependentTasks = new HashSet<>();

        public String getId()
        {
            return id;
        }

        public void setId(String id)
        {
            this.id = id;
        }

        public Long getCreatedDate()
        {
            return createdDate;
        }

        public void setCreatedDate(Long createdDate)
        {
            this.createdDate = createdDate;
        }

        public String getReferenceId()
        {
            return referenceId;
        }

        public void setReferenceId(String referenceId)
        {
            this.referenceId = referenceId;
        }

        public String getParentReferenceId()
        {
            return parentReferenceId;
        }

        public void setParentReferenceId(String parentReferenceId)
        {
            this.parentReferenceId = parentReferenceId;
        }

        public String getStatus()
        {
            return status;
        }

        public void setStatus(String status)
        {
            this.status = status;
        }

        public void addDependentTask(SubTask obj)
        {
            this.dependentTasks.add(obj);
        }
    }
}
