package compute.job;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import compute.configure.AllConfiguration;
import compute.configure.TaskTrackerConfiguration;
import compute.task.TaskTracker;
import compute.utility.Host;

/*
 * TaskTrackerTable.java
 * 
 * Author: San-Chuan Hung
 * 
 * This class manage task trackers(participants) in coordinator. 
 * 
 * */

public class TaskTrackerTable implements Serializable{
  
  public int MAXTasktrackerId = 0;
  
  public Map<String, TaskTrackerTableItem> taskTrackerMap;

  public TaskTrackerTable() {
    this.taskTrackerMap = new HashMap<String, TaskTrackerTableItem>();
    MAXTasktrackerId = 0;
  }
  
  public int newTaskTrackerId(){
    int thisTaskTrackerId = MAXTasktrackerId;
    MAXTasktrackerId += 1;
    return thisTaskTrackerId;
  }
  
  public List<String> checkDeadTaskTrackerIds(){
    List<String> deadTaskTrackerIds = null;
    for(String taskTrackerId: this.taskTrackerMap.keySet()){
      TaskTrackerTableItem item = this.taskTrackerMap.get(taskTrackerId);
      if((System.currentTimeMillis()/1000 -  item.getLastUpdateTime()) > AllConfiguration.taskTrackerDieOutTime){
        if(deadTaskTrackerIds == null){ deadTaskTrackerIds = new ArrayList<String>();}
        deadTaskTrackerIds.add(taskTrackerId);
      }
    }
    return deadTaskTrackerIds;
  }

  public long updateTime(String taskTrackerId) {
    TaskTrackerTableItem item = this.taskTrackerMap.get(taskTrackerId);
    return item.updateTime();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (TaskTrackerTableItem item : this.taskTrackerMap.values()) {
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

  public void put(String id, TaskTracker taskTracker) throws Exception {
    Host host = new Host(taskTracker.getHostName(), taskTracker.getPort());
    this.taskTrackerMap.put(id, new TaskTrackerTableItem(taskTracker, host));
  }

  public void add(TaskTracker taskTracker) throws Exception{
    String taskTrackerId = Integer.toString(newTaskTrackerId());
    taskTracker.setTaskTrackerId(taskTrackerId);
    this.put(taskTrackerId, taskTracker);
  }
  
  public void removeAll(List<String> taskTrackerIds){
    for(String taskTrackerId : taskTrackerIds){
      this.remove(taskTrackerId);
    }
  }
  
  public void remove(String taskTrackerId)  {
    this.taskTrackerMap.remove(taskTrackerId);
  }
    
  public TaskTracker get(String id){
    TaskTrackerTableItem item = this.taskTrackerMap.get(id);
    return item.getTaskTracker();
  }
  
  public TaskTracker get(Host host) {
    for (TaskTrackerTableItem taskTrackerItem : taskTrackerMap.values()) {

      if (taskTrackerItem.getHost().equals(host)) {
        return taskTrackerItem.getTaskTracker();
      }
    }
    return null;
  } 
  
  public Host getHost(String id){
    TaskTrackerTableItem item = this.taskTrackerMap.get(id);
    if(item == null){return null;}
    return item.getHost();
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

  public void setTaskTrackerStats(String taskTrackerId, TaskTrackerStats taskTrackerStats) {
    TaskTrackerTableItem item = this.taskTrackerMap.get(taskTrackerId);
    item.setTaskTrackerStats(taskTrackerStats);
  }

  public Host getAvaliableReducerHost() {
    for (TaskTrackerTableItem item : this.taskTrackerMap.values()) {
      TaskTrackerStats stats = item.getStats();
      if (stats.getMapTaskSlot() == TaskTrackerConfiguration.maxNumOfMapper
              && stats.getReducePreprcoessSlot() > 0) {
        return item.host;
      }
    }
    return null;
  }

}
