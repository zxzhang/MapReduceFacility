package compute.job;

import java.rmi.Remote;
import java.rmi.RemoteException;

import compute.job.message.HeartbeatMessage;
import compute.job.message.JobTrackMessage;
import compute.job.message.MonitorMessage;
import compute.mapper.Mapper;
import compute.reducer.Reducer;
import compute.task.TaskTracker;




public interface JobTracker extends  Remote {
//  public MonitorMessage monitorJob(String jobId) throws RemoteException;
  
  public String submitJob(String dfsInputPath, Class<? extends Mapper> mapper, Class<? extends Reducer> reducer) throws RemoteException;

  public JobTrackMessage trackJob(String jobId) throws RemoteException;
  
  public boolean register(TaskTracker taskTracker) throws RemoteException;
  
  public boolean heartbeat(String taskTrackerId, HeartbeatMessage hbm) throws RemoteException;
  
}
