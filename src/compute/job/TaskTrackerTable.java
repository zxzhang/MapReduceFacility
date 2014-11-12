package compute.job;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

import compute.configure.AllConfiguration;
import compute.configure.TaskTrackerConfiguration;
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
  
  public void setTaskTrackerStats(TaskTrackerStats taskTrackerStats){
    this.taskTrackerStats = taskTrackerStats;
  }
}

public class TaskTrackerTable {
  public Map<String, TaskTrackerTableItem> taskTrackerMap;
  
  public TaskTrackerTable(){
    this.taskTrackerMap = new HashMap<String, TaskTrackerTableItem>();
  }
  
  public long updateTime(String taskTrackerId){
    TaskTrackerTableItem item = this.taskTrackerMap.get(taskTrackerId);
    return item.updateTime();  
  }
  
  public String toString(){
    StringBuilder sb= new StringBuilder();
    for(TaskTrackerTableItem item: this.taskTrackerMap.values()){
      try {
        sb.append(item.taskTracker.getHostName());
      } catch (RemoteException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      sb.append("\t");
      try {
        sb.append(item.taskTracker.getPort());
      } catch (RemoteException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return sb.toString();
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
  public TaskTrackerStats getTaskTrackerStats(String taskTrackerId){
    TaskTrackerTableItem item = this.taskTrackerMap.get(taskTrackerId);
    return item.taskTrackerStats;
  }
  
  public void setTaskTrackerStats(String taskTrackerId, TaskTrackerStats taskTrackerStats){
    TaskTrackerTableItem item = this.taskTrackerMap.get(taskTrackerId);
    item.setTaskTrackerStats(taskTrackerStats);
  }
  
  public Host getAvaliableReducerHost(){
    for(TaskTrackerTableItem item: this.taskTrackerMap.values()){
      TaskTrackerStats stats = item.getStats();
      System.out.println(stats);
      if(stats.getMapTaskSlot() == TaskTrackerConfiguration.maxNumOfMapper 
          && stats.getReducePreprcoessSlot() > 0){
        return item.host;
      }
    }
    return null;
  }

}
