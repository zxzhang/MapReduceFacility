package compute.job.message;

import java.io.Serializable;

import compute.job.TaskTrackerStats;

/*
 * HeartbeatMessage.java
 * 
 * Author: San-Chuan Hung
 * 
 * Task tracker(participant) sends it's heartbeat with this class
 * 
 * */

public class HeartbeatMessage implements Serializable{
  int mapTaskSlot;
  int reducePreprocessTaskSlot;
  int reduceTaskSlot; 
  
  TaskTrackerStats taskTrackerStats;
  
  public HeartbeatMessage(int mapTaskSlot, int reducePreprocessTaskSlot, int reduceTaskSlot){
    
    this.mapTaskSlot = mapTaskSlot;
    this.reducePreprocessTaskSlot = reducePreprocessTaskSlot;
    this.reduceTaskSlot = reduceTaskSlot;
    
    this.taskTrackerStats = new TaskTrackerStats(mapTaskSlot, reducePreprocessTaskSlot, reduceTaskSlot);
  }
  
  public int getMapTaskSlot(){
    return mapTaskSlot;
  }
  public int getReducePreprocessTaskSlot(){
    return reducePreprocessTaskSlot;
  }
  public int getReduceTaskSlot(){
    return reduceTaskSlot;
  }
  public TaskTrackerStats getTaskTrackerStats(){
    return taskTrackerStats;
  }
}
