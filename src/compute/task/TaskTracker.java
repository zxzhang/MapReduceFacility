package compute.task;

import java.rmi.Remote;
import java.rmi.RemoteException;

import compute.job.JobTracker;


public interface TaskTracker extends Remote {
  public String getTaskTrackerId() throws RemoteException;
  public void ack() throws RemoteException;  
  public String getHostName() throws RemoteException;
  public int getPort() throws RemoteException;
  public boolean assignMapTask(MapTask task) throws RemoteException;
  public boolean assignReducePreprocessTask(ReducePreprocessTask task) throws RemoteException;
  public boolean assignReduceTask(ReduceTask task) throws RemoteException;
}
