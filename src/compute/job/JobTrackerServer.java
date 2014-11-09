package compute.job;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import compute.job.message.HeartbeatMessage;
import compute.job.message.JobTrackMessage;
import compute.mapper.Mapper;
import compute.reducer.Reducer;
import compute.task.TaskTracker;


public class JobTrackerServer implements JobTracker {
  JobTable jobTable;
  TaskTrackerTable taskTrackerTable;
  
  public String submitJob( String dfsInputPath, String dfsOutputPath,
      Class<? extends Mapper> mapper, Class<? extends Reducer> reducer){
    
    return jobTable.addJob(dfsInputPath, dfsOutputPath, mapper, reducer);
  }

  public JobTrackMessage trackJob(String jobId){
    Job job = jobTable.get(jobId);
    return new JobTrackMessage(job);
  }
    
  public JobTrackerServer(){
    jobTable = new JobTable();
    taskTrackerTable = new TaskTrackerTable();
  }
  
  public boolean register(TaskTracker taskTracker) throws RemoteException{
    try{
      taskTrackerTable.put(taskTracker.getTaskTrackerId(), taskTracker);
      taskTracker.ack();
      return true;
    }catch(Exception e){
      return false;
    }
  }
  
  public static void main(String[] args) {
    String host = "localhost";
    //launch JobTrackerServer 
    try {
      System.out.println("Server init.");
      JobTrackerServer obj = new JobTrackerServer();
      JobTracker stub = (JobTracker) UnicastRemoteObject.exportObject(obj, 0);
      // Bind the remote object's stub in the RMI registry
      Registry registry = LocateRegistry.getRegistry();
      registry.rebind("jobtracker", stub);
      System.out.println("Server ready");
    } catch (RemoteException e) {
      System.err.println("Server exception[RemoteException] : " + e.toString());
      e.printStackTrace();
    }
  }

  @Override
  public boolean heartbeat(String taskTrackerId, HeartbeatMessage hbm) throws RemoteException {
    // update TaskTracker updated time.
    taskTrackerTable.updateTime(taskTrackerId);
    
    return true;
  }




}
