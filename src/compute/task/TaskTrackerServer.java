package compute.task;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import compute.job.JobTracker;

public class TaskTrackerServer implements TaskTracker {
  String taskTrackerId;
  String hostName;
  public String getTaskTrackerId(){
    return this.taskTrackerId;
  }
 
  public TaskTrackerServer(String taskTrackerId, String hostName){
    this.taskTrackerId = taskTrackerId;
    this.hostName = hostName;
  }
  
  public static String getHostName() throws UnknownHostException{
    return InetAddress.getLocalHost().getHostName();
  }
  
  public static void main(String[] args) throws Exception{
    
    
    // launch local server for task tracker
    String localHostName = getHostName();
    TaskTracker taskTracker = new TaskTrackerServer(localHostName, localHostName);
    TaskTracker stub = (TaskTracker)UnicastRemoteObject.exportObject(taskTracker, 0);
    Registry registry = LocateRegistry.getRegistry();
    registry.rebind("tasktracker", stub);
    
    // args[0] = job tracker host
    String jobTrackerHost = (args.length < 1) ? null : args[0];
    try {
        Registry remoteRegistry = LocateRegistry.getRegistry(jobTrackerHost);
        JobTracker jobTracker = (JobTracker) remoteRegistry.lookup("jobtracker");
        
        
        
    } catch (Exception e) {
        System.err.println("Client exception: " + e.toString());
        e.printStackTrace();
    }
  }
}
