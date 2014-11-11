package compute.job.message;

import java.io.Serializable;

public class HeartbeatMessage implements Serializable{
  int mapTaskSlot;
  int reducePreprocessTaskSlot;
  int reduceTaskSlot;
  
  public HeartbeatMessage(int mapTaskSlot, int reducePreprocessTaskSlot, int reduceTaskSlot){
    this.mapTaskSlot = mapTaskSlot;
    this.reducePreprocessTaskSlot = reducePreprocessTaskSlot;
    this.reduceTaskSlot = reduceTaskSlot;
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
}
