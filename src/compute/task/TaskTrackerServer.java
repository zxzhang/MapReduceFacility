package compute.task;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Deque;
import java.util.Iterator;

import compute.configure.TaskTrackerConfiguration;
import compute.job.JobTracker;
import compute.job.message.HeartbeatMessage;
import compute.task.box.Callback;
import compute.task.box.MapCallback;
import compute.task.box.MapTaskBox;
import compute.test.DFS;
import compute.test.FakeDFS;
import compute.utility.HostUtility;
import compute.utility.LocalIOUtility;


class MapTaskItem{
  MapTask task;
}

public class TaskTrackerServer implements TaskTracker {
  String taskTrackerId;
  String hostName;
  int port;
  JobTracker jobTracker;
  String localSpacePath;
  DFS dfs;
  
  Deque<MapTask> pendingMapTasks;
  Deque<ReducePreprocessTask> pendingReducePreprocessTasks;
  Deque<ReduceTask> pendingReduceTasks;

  Deque<MapTask> runningMapTasks;
  Deque<ReducePreprocessTask> runningReducePreprocessTasks;
  Deque<ReduceTask> runningReduceTasks;
  
  Deque<MapTask> finishedMapTasks;
  Deque<ReducePreprocessTask> finishedReducePreprocessTasks;
  Deque<ReduceTask> finishedReduceTasks;  
  
  
  public void setJobTracker(JobTracker jobTracker){
    this.jobTracker = jobTracker;
  }
  
  public void setDFS(DFS dfs){
    this.dfs = dfs;
  }
  
  public String getHostName(){
    return this.getHostName();
  }
  
  public int getPort(){
    return this.getPort();
  }
  
  public String getTaskTrackerId(){
    return this.taskTrackerId;
  }
 
  public TaskTrackerServer(String taskTrackerId, String hostName, int port){
    this.taskTrackerId = taskTrackerId;
    this.hostName = hostName;
    this.port = port;

    this.localSpacePath = LocalIOUtility.getLocalSpace(this);
  }
  
  public void ack(){
    System.out.println("Register OK.");
  }
  
  public void checkPendingMapTask(){
    MapCallback callback = new MapCallback(this);
    
    Iterator<MapTask> mapTasksIter = pendingMapTasks.iterator();
    while(mapTasksIter.hasNext()){
      MapTask task = mapTasksIter.next();
      // create MapTaskBox
      MapTaskBox taskBox = new MapTaskBox(task, dfs, callback);
      // run taskbox
      taskBox.start();
      // move to running MapTask
      mapTasksIter.remove();
      addRunningMapTask(task);
    }
  }
  
  public void run() throws InterruptedException, RemoteException{
    while(true){ // the loop executes each 1 secs
      // check 
      checkPendingMapTask();
      
      // report heartbeat
      int mapTaskSlot = getMapTaskSlot();
      int reducePreprocessTaskSlot = getReducePreprocessTaskSlot();
      int reduceTaskSlot = getReduceTaskSlot();
      this.jobTracker.heartbeat(
          this.getTaskTrackerId(), 
          new HeartbeatMessage(mapTaskSlot, reducePreprocessTaskSlot, reduceTaskSlot)
      );
      
      Thread.sleep(1000);  
    }
  }
  
  public int getMapTaskSlot(){
    return TaskTrackerConfiguration.maxNumOfMapper - (this.pendingMapTasks.size() + this.runningMapTasks.size());
  }
  public int getReducePreprocessTaskSlot(){
    return TaskTrackerConfiguration.maxNumOfReducePreprocess - (this.pendingReducePreprocessTasks.size() + this.runningReducePreprocessTasks.size());
  }
  public int getReduceTaskSlot(){
    return TaskTrackerConfiguration.maxNumOfReducer - (this.pendingReduceTasks.size() + this.runningReduceTasks.size());
  }
  
  public boolean assignMapTask(MapTask task){
    if(getMapTaskSlot()>0){
      addPendingMapTask(task);
      return true;
    }
    return false;
  }
  
  public void addPendingMapTask(MapTask task){
    String[] tmp = task.getDfsInputPath().split("/");
    String subFilename = tmp[tmp.length-1];
    
    task.setLocalOutputPath(String.format("%s/%s", localSpacePath, subFilename));
    this.pendingMapTasks.add(task);
  }
  public void removePendingMapTask(MapTask task){
    this.pendingMapTasks.remove(task);
  }
  public void addRunningMapTask(MapTask task){
    this.runningMapTasks.add(task);
  }
  public void removeRunningMapTask(MapTask task){
    this.runningMapTasks.remove(task);
  }
  public void addFinishedMapTask(MapTask task){
    this.finishedMapTasks.add(task);
  }
  public void removeFinishedMapTask(MapTask task){
    this.finishedMapTasks.remove(task);
  }
  public void addPendingReducePreprocessTask(ReducePreprocessTask task){
    this.pendingReducePreprocessTasks.add(task);
  }
  public void removePendingReducePreprocessTask(ReducePreprocessTask task){
    this.pendingReducePreprocessTasks.remove(task);
  }
  public void addRunningReducePreprocessTask(ReducePreprocessTask task){
    this.runningReducePreprocessTasks.add(task);
  }
  public void removeRunningReducePreprocessTask(ReducePreprocessTask task){
    this.runningReducePreprocessTasks.remove(task);
  }
  public void addFinishedReducePreprocessTask(ReducePreprocessTask task){
    this.finishedReducePreprocessTasks.add(task);
  }
  public void removeFinishedReducePreprocessTask(ReducePreprocessTask task){
    this.finishedReducePreprocessTasks.remove(task);
  }
  public void addPendingReduceTask(ReduceTask task){
    this.pendingReduceTasks.add(task);
  }
  public void removePendingReduceTask(ReduceTask task){
    this.pendingReduceTasks.remove(task);
  }
  public void addRunningReduceTask(ReduceTask task){
    this.runningReduceTasks.add(task);
  }
  public void removeRunningReduceTask(ReduceTask task){
    this.runningReduceTasks.remove(task);
  }
  public void addFinishedReduceTask(ReduceTask task){
    this.finishedReduceTasks.add(task);
  }
  public void removeFinishedReduceTask(ReduceTask task){
    this.finishedReduceTasks.remove(task);
  }
  
  public boolean assignReducePreprocessTask(ReducePreprocessTask task){
    if(getReducePreprocessTaskSlot()>0){
      addPendingReducePreprocessTask(task);
      return true;
    }
    return false;
  }
  public boolean assignReduceTask(ReduceTask task){
    if(getReduceTaskSlot() > 0){
      addPendingReduceTask(task);
      return true;
    }
    return false;
  }
  
  public static void main(String[] args) throws Exception{
    String jobTrackerHost = args[0];
    int jobTrackerPort = 1099;
    int taskTrackerPort = Integer.parseInt(args[1]);
    
    // launch local server for task tracker
    String localHostName = HostUtility.getHostName();
    TaskTrackerServer taskTracker = new TaskTrackerServer(localHostName, localHostName, taskTrackerPort);
    TaskTracker stub = (TaskTracker)UnicastRemoteObject.exportObject(taskTracker, 0);
    Registry registry = LocateRegistry.getRegistry(taskTrackerPort);
    registry.rebind("tasktracker", stub);
    
    // args[0] = job tracker host
    // build connection with JobTracker
    try {
        Registry remoteRegistry = LocateRegistry.getRegistry(jobTrackerHost, jobTrackerPort);
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
    // build connection with DFS
    DFS dfs = FakeDFS.getConnection("localhost", 8888);
    taskTracker.setDFS(dfs);
    
    // run taskTracker
    taskTracker.run();
  }


}
