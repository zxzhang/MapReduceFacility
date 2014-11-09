package compute.task;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import compute.configure.TaskTrackerConfiguration;
import compute.job.JobTracker;
import compute.job.message.HeartbeatMessage;

public class TaskTrackerServer implements TaskTracker {
  String taskTrackerId;
  String hostName;
  JobTracker jobTracker;
  int numOfMapperSlot;
  int numOfReducerSlot;
  
  public void setJobTracker(JobTracker jobTracker){
    this.jobTracker = jobTracker;
  }
  
  public String getTaskTrackerId(){
    return this.taskTrackerId;
  }
 
  public TaskTrackerServer(String taskTrackerId, String hostName){
    this.taskTrackerId = taskTrackerId;
    this.hostName = hostName;
    this.numOfMapperSlot = TaskTrackerConfiguration.maxNumOfMapper;
    this.numOfReducerSlot = TaskTrackerConfiguration.maxNumOfReducer;
  }
  
  public static String getHostName() throws UnknownHostException{
    return InetAddress.getLocalHost().getHostName();
  }
  public void ack(){
    System.out.println("Register OK.");
  }
  
  public void run() throws InterruptedException, RemoteException{
    while(true){
    
      this.jobTracker.heartbeat(
          this.getTaskTrackerId(), new HeartbeatMessage(this.numOfMapperSlot, this.numOfReducerSlot));
      
      Thread.sleep(1000);  
    }
  }
  
  public static void main(String[] args) throws Exception{
    
    
    // launch local server for task tracker
    String localHostName = getHostName();
    TaskTrackerServer taskTracker = new TaskTrackerServer(localHostName, localHostName);
    TaskTracker stub = (TaskTracker)UnicastRemoteObject.exportObject(taskTracker, 0);
    Registry registry = LocateRegistry.getRegistry();
    registry.rebind("tasktracker", stub);
    
    // args[0] = job tracker host
    // build connection with JobTracker
    String jobTrackerHost = (args.length < 1) ? null : args[0];
    try {
        Registry remoteRegistry = LocateRegistry.getRegistry(jobTrackerHost);
        JobTracker jobTracker = (JobTracker) remoteRegistry.lookup("jobtracker");
        if(!jobTracker.register(stub)){
          System.err.println("Cannot register JobTracker: " + jobTrackerHost);
          System.exit(0);
        }        
        taskTracker.setJobTracker(jobTracker);
    } catch (Exception e) {
        System.err.println("Client exception: " + e.toString());
        e.printStackTrace();
    }
    // run taskTracker
    taskTracker.run();
  }
}
