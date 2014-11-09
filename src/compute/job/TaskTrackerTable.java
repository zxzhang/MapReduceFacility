package compute.job;

import java.util.HashMap;
import java.util.Map;

import compute.task.TaskTracker;

public class TaskTrackerTable {
  private Map<String, TaskTracker> taskTrackerMap;
  public TaskTrackerTable(){
    this.taskTrackerMap = new HashMap<String, TaskTracker>();
  }
  public void put(String id, TaskTracker taskTracker){
    this.taskTrackerMap.put(id,  taskTracker);
  }
  public TaskTracker get(String id){
    return this.taskTrackerMap.get(id);
  }
}
