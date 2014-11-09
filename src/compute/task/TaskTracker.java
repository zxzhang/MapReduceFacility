package compute.task;

import java.rmi.Remote;


public interface TaskTracker extends Remote {
  public String getTaskTrackerId();
  
}
