package compute.job;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
  
  public long getLastUpdateTime(){
    return lastUpdateTime;
  }
}

public class TaskTrackerTable {
  
  public int MAXTasktrackerId = 0;
  
  public Map<String, TaskTrackerTableItem> taskTrackerMap;
  
  public TaskTrackerTable(){
    this.taskTrackerMap = new HashMap<String, TaskTrackerTableItem>();
    MAXTasktrackerId = 0;
  }
  
  public int newTaskTrackerId(){
    int thisTaskTrackerId = MAXTasktrackerId;
    MAXTasktrackerId += 1;
    return thisTaskTrackerId;
  }
  
  public List<TaskTracker> checkDeadTaskTrackers(){
    List<TaskTracker> deadTaskTrackers = null;
    for(TaskTrackerTableItem item: this.taskTrackerMap.values()){
      if((System.currentTimeMillis()/1000 -  item.getLastUpdateTime()) > AllConfiguration.taskTrackerDieOutTime){
        if(deadTaskTrackers == null){ deadTaskTrackers = new ArrayList<TaskTracker>();}
        deadTaskTrackers.add(item.getTaskTracker());
      }
    }
    return deadTaskTrackers;
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
  
  public void add(TaskTracker taskTracker) throws Exception{
    String taskTrackerId = Integer.toString(newTaskTrackerId());
    taskTracker.setTaskTrackerId(taskTrackerId);
    this.put(taskTrackerId, taskTracker);
  }
  
//  
//  public void removeAll(List<TaskTracker> taskTrackers){
//    for(TaskTracker taskTracker : taskTrackers){
//      this.remove(taskTracker.getTaskTrackerId());
//    }
//  }
//  
  public void remove(String taskTrackerId)  {
    this.taskTrackerMap.remove(taskTrackerId);
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
      if(stats.getMapTaskSlot() == TaskTrackerConfiguration.maxNumOfMapper 
          && stats.getReducePreprcoessSlot() > 0){
        return item.host;
      }
    }
    return null;
  }

}
