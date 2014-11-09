package compute.task;

import java.rmi.Remote;
import java.rmi.RemoteException;

import compute.job.JobTracker;


public interface TaskTracker extends Remote {
  public String getTaskTrackerId() throws RemoteException;
  public void ack() throws RemoteException;  
}
