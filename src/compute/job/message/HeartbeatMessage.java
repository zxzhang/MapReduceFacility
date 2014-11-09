package compute.job.message;

import java.io.Serializable;

public class HeartbeatMessage implements Serializable{
  public int numOfMapperSlot;
  public int numOfReducerSlot;
  
  public HeartbeatMessage(int numOfMapperSlot, int numOfReducerSlot){
    this.numOfMapperSlot = numOfMapperSlot;
    this.numOfReducerSlot = numOfReducerSlot;
  }
  
  public int getNumOfMapperSlot(){
    return numOfMapperSlot;
  }
  
  public int getNumOfReducerSlot(){
    return numOfReducerSlot;
  }
}
