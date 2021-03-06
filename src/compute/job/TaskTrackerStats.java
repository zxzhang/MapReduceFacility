package compute.job;

import java.io.Serializable;

/*
 * TaskTrackerStats.java
 * 
 * Author: San-Chuan Hung
 * 
 * The task tracker slot statistic.
 * 
 * */

public class TaskTrackerStats implements Serializable{
  int mapTaskSlot;
  int reduceTaskSlot;
  int reducePreprcoessTaskSlot;
  public TaskTrackerStats(int mapTaskSlot, int reducePreprcoessTaskSlot, int reduceTaskSlot){
    this.mapTaskSlot = mapTaskSlot;
    this.reduceTaskSlot = reduceTaskSlot;
    this.reducePreprcoessTaskSlot = reducePreprcoessTaskSlot;
  }
  public int getMapTaskSlot(){return mapTaskSlot;}
  public int getReduceTaskSlot(){return reduceTaskSlot;}
  public int getReducePreprcoessSlot(){return reducePreprcoessTaskSlot;}
  
  public void setMapTaskSlot(int mapTaskSlot){ this.mapTaskSlot = mapTaskSlot;}
  public void setReduceTaskSlot(int reduceTaskSlot){ this.reduceTaskSlot = reduceTaskSlot;}
  public void setReducePreprocessSlot(int reducePreprcoessTaskSlot){ this.reducePreprcoessTaskSlot = reducePreprcoessTaskSlot;}
  
  public String toString(){
    return String.format("m: %d\tr_p: %d\tr: %d", mapTaskSlot, reducePreprcoessTaskSlot, reduceTaskSlot);
  }
}
