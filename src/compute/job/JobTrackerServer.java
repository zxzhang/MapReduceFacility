package compute.job;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import compute.job.message.JobTrackMessage;


public class JobTrackerServer implements JobTracker {
  JobTable jobTable;
  
  public String submitJob( String dfsInputPath, 
      Class<? extends Mapper> mapper, Class<? extends Reducer> reducer){
    
    return jobTable.addJob(dfsInputPath, mapper, reducer);
  }

  public JobTrackMessage trackJob(String jobId){
    Job job = jobTable.get(jobId);
    return new JobTrackMessage(job);
  }
    
  public JobTrackerServer(){
    jobTable = new JobTable();
  }
  
  public static void main(String[] args) {
    String host = args[0];
    try {
      System.out.println("Server init.");
      JobTrackerServer obj = new JobTrackerServer();
      JobTracker stub = (JobTracker)UnicastRemoteObject.exportObject(obj, 0);
      // Bind the remote object's stub in the RMI registry
      Registry registry = LocateRegistry.getRegistry();
      registry.rebind("jobtracker", stub);
      System.out.println("Server ready");
    } catch (RemoteException e) {
      System.err.println("Server exception[RemoteException] : " + e.toString());
      e.printStackTrace();
    }
  }

}
