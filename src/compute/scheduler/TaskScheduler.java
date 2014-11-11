package compute.scheduler;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import compute.job.Job;
import compute.job.TaskTrackerStats;
import compute.job.TaskTrackerTable;
import compute.task.*;
import compute.test.DFS;
import compute.utility.Host;

public class TaskScheduler {
  Deque<MapTask> pendingMapTasks;
  Deque<ReducePreprocessTask> pendingReducePreprocessTasks;
  Deque<ReduceTask> pendingReduceTasks;
  
  Deque<MapTask> runningMapTasks;
  Deque<ReducePreprocessTask> runningReducePreprocessTasks;
  Deque<ReduceTask> runningReduceTasks;
  
  Deque<MapTask> finishedMapTasks;
  Deque<ReducePreprocessTask> finishedReducePreprocessTasks;
  Deque<ReduceTask> finishedReduceTasks;
  
//  Queue<Task> pendingTasks;
//  Queue<Task> runningTasks;
//  Queue<Task> finishedTasks;
//  
  DFS dfs;
  TaskTrackerTable taskTrackerTable;

  
  public TaskScheduler(DFS dfs, TaskTrackerTable taskTrackerTable){
    this.pendingMapTasks = new LinkedList<MapTask>();
    this.pendingReducePreprocessTasks = new LinkedList<ReducePreprocessTask>();
    this.pendingReduceTasks = new LinkedList<ReduceTask>();

    this.runningMapTasks = new LinkedList<MapTask>();
    this.runningReducePreprocessTasks = new LinkedList<ReducePreprocessTask>();
    this.runningReduceTasks = new LinkedList<ReduceTask>();
 
    this.finishedMapTasks = new LinkedList<MapTask>();
    this.finishedReducePreprocessTasks = new LinkedList<ReducePreprocessTask>();
    this.finishedReduceTasks = new LinkedList<ReduceTask>();
    
    this.dfs = dfs;
    this.taskTrackerTable = taskTrackerTable;

  }
  
  public int getPenndingMapTasksSize(){
    return this.pendingMapTasks.size();
  }
  
  public int getPenndingReduceTasksSize(){
    return this.pendingReduceTasks.size();
  }
  public int getPenndingReducePreprocessTasksSize(){
    return this.pendingReducePreprocessTasks.size();
  }
  
  public boolean addPendingMapTask(MapTask task){
    task.setTaskStatus(TaskStatus.PENDING);
    this.pendingMapTasks.add(task);
    return true;
  }
  public boolean addRunningMapTask(MapTask task){
    task.setTaskStatus(TaskStatus.RUNNING);
    this.runningMapTasks.add(task);
    return true;
  }
  public boolean addFinishedMapTask(MapTask task){
    task.setTaskStatus(TaskStatus.FINISHED);
    this.finishedMapTasks.add(task);
    return true;
  }
  public boolean addPendingReducePreprocessTask(ReducePreprocessTask task){
    task.setTaskStatus(TaskStatus.PENDING);
    this.pendingReducePreprocessTasks.add(task);
    return true;
  }
  public boolean addRunningReducePreprocessTask(ReducePreprocessTask task){
    task.setTaskStatus(TaskStatus.RUNNING);
    this.runningReducePreprocessTasks.add(task);
    return true;
  }
  public boolean addFinishedunningReducePreprocessTask(ReducePreprocessTask task){
    task.setTaskStatus(TaskStatus.FINISHED);
    this.finishedReducePreprocessTasks.add(task);
    return true;
  }  
  public boolean addPendingReducePreprocessTask(ReduceTask task){
    task.setTaskStatus(TaskStatus.PENDING);
    this.pendingReduceTasks.add(task);
    return true;
  }
  public boolean addRunningReducePreprocessTask(ReduceTask task){
    task.setTaskStatus(TaskStatus.RUNNING);
    this.runningReduceTasks.add(task);
    return true;
  }
  public boolean addFinishedunningReducePreprocessTask(ReduceTask task){
    task.setTaskStatus(TaskStatus.FINISHED);
    this.finishedReduceTasks.add(task);
    return true;
  }    
  
  List<MapTask> splitJobToMapTaskList(Job job){
    List<MapTask> mapTaskList = new ArrayList<MapTask>();
    List<String> splitInputFiles = dfs.ls(job.getDfsInputPath());
    for(String dfsInputPath: splitInputFiles){
      MapTask mapTask = new MapTask(dfsInputPath, job.getMapper());
      mapTask.setJob(job);
      mapTaskList.add(mapTask);
    }
    return mapTaskList;
  }
  
  public List<MapTask> addJob(Job job){
    // split job into map tasks. insert them into pending tasks 
    List<MapTask> mapTaskList = splitJobToMapTaskList(job);
    for(MapTask mapTask: mapTaskList){
      this.addPendingMapTask(mapTask);
    }
    return mapTaskList;
  }
  
  public boolean finishMapTask(MapTask task){
    this.pendingMapTasks.remove(task);
    this.runningMapTasks.remove(task);
    this.addFinishedMapTask(task);
    return true;
  }
  
  public boolean schedulePendingMapTask() {
    Iterator<MapTask> pendingMapTasks = this.pendingMapTasks.iterator();
    
//    System.out.println("pendingMapTasks size:" + this.pendingMapTasks.size());
    
    while(pendingMapTasks.hasNext()){
      MapTask task = pendingMapTasks.next();
      
      for(int version = 0; version < 3; version++){
        // search near by data node  
        Host host = dfs.getHost(task.getDfsInputPath(), version);
        
        // check whether the node have map slot
        TaskTrackerStats stats = taskTrackerTable.getTaskTrackerStats(host);
        if(stats != null && stats.getMapTaskSlot() > 0){
          // assign task
          TaskTracker taskTracker = taskTrackerTable.get(host);  
          
          System.out.println("taskTracker: " + host);
          
          try {
            if(taskTracker.assignMapTask(task)){
              // remove from pending queue
              pendingMapTasks.remove();
              // add into running queue
              addRunningMapTask(task);
              
              break; // if success                   
            }
          } catch (RemoteException e) {
             // cannot assign the task, because it cannot connect to the remote host 
            e.printStackTrace();
            continue;
          }
        }
      }
    }    
    return true;
  }
  
  public String toString(){
    return String.format(
        "map\np: %s\nr: %s\nf: %s", 
        this.pendingMapTasks.toString(), 
        this.runningMapTasks.toString(), 
        this.finishedMapTasks.toString()
    );
  }
}
