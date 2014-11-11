package compute.job;

public class TaskTrackerStats {
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
}
