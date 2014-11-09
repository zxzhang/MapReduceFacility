package compute.job;

import java.util.HashMap;
import java.util.Map;

import compute.task.TaskTracker;
class TaskTrackerTableItem{
  
  TaskTracker taskTracker;
  long lastUpdateTime; // secs
  
  public TaskTrackerTableItem(TaskTracker taskTracker){
    this.taskTracker = taskTracker;
    this.updateTime();
  }
  
  public long updateTime(){
    this.lastUpdateTime = System.currentTimeMillis()/ 1000 ;
    return lastUpdateTime;
  }
  
  public TaskTracker getTaskTracker(){
    return taskTracker;
  }
}

public class TaskTrackerTable {
  private Map<String, TaskTrackerTableItem> taskTrackerMap;
  
  public TaskTrackerTable(){
    this.taskTrackerMap = new HashMap<String, TaskTrackerTableItem>();
  }
  
  public void put(String id, TaskTracker taskTracker){
    this.taskTrackerMap.put(id, new TaskTrackerTableItem(taskTracker));
  }
  
  public TaskTracker get(String id){
    TaskTrackerTableItem item =  this.taskTrackerMap.get(id);
    return item.getTaskTracker();
  }
  
  public long updateTime(String taskTrackerId){
    
    TaskTrackerTableItem item = this.taskTrackerMap.get(taskTrackerId);
    return item.updateTime();  
  }
}
