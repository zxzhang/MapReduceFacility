package compute.job;

import java.util.HashMap;
import java.util.Map;

import compute.task.TaskTracker;
import compute.utility.Host;


class TaskTrackerTableItem{
  
  TaskTracker taskTracker;
  long lastUpdateTime; // secs
  Host host;
  TaskTrackerStats taskTrackerStats;
  
  public TaskTrackerTableItem(TaskTracker taskTracker, Host host){
    this.taskTracker = taskTracker;
    this.host = host;
    this.updateTime();
  }
  
  public Host getHost(){return host;}
  
  public long updateTime(){
    this.lastUpdateTime = System.currentTimeMillis()/ 1000 ;
    return lastUpdateTime;
  }
  
  public TaskTracker getTaskTracker(){
    return taskTracker;
  }
  
  public TaskTrackerStats getStats(){
    return taskTrackerStats;
  }
}

public class TaskTrackerTable {
  private Map<String, TaskTrackerTableItem> taskTrackerMap;
  
  public TaskTrackerTable(){
    this.taskTrackerMap = new HashMap<String, TaskTrackerTableItem>();
  }
  
  public void put(String id, TaskTracker taskTracker) throws Exception{
    Host host = new Host(taskTracker.getHostName(), taskTracker.getPort());
    this.taskTrackerMap.put(id, new TaskTrackerTableItem(taskTracker, host));
  }
  
  public TaskTracker get(String id){
    TaskTrackerTableItem item = this.taskTrackerMap.get(id);
    return item.getTaskTracker();
  }
  public TaskTracker get(Host host){
    for(TaskTrackerTableItem taskTrackerItem: taskTrackerMap.values()){
       if(taskTrackerItem.getHost().equals(host)){
         return taskTrackerItem.getTaskTracker();
       }
    }
    return null;
  }
  
  public TaskTrackerStats getTaskTrackerStats(Host host){
    for(TaskTrackerTableItem taskTrackerItem: taskTrackerMap.values()){
      if(taskTrackerItem.getHost().equals(host)){
        return taskTrackerItem.getStats();
      }
    }
    return null;
  }
  
  public long updateTime(String taskTrackerId){
    TaskTrackerTableItem item = this.taskTrackerMap.get(taskTrackerId);
    return item.updateTime();  
  }
}
