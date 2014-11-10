package compute.scheduler;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import compute.job.Job;
import compute.task.*;
import compute.test.DFS;

public class TaskScheduler {
  Queue<Task> pendingTasks;
  Queue<Task> runningTasks;
  Queue<Task> finishedTasks;
  
  DFS dfs;
  
  public TaskScheduler(DFS dfs){
    this.pendingTasks = new LinkedList<Task>();
    this.runningTasks = new LinkedList<Task>();
    this.finishedTasks = new LinkedList<Task>();
    this.dfs = dfs;
  }
  
  List<Task> splitJobToMapTaskList(Job job){
    
    List<Task> mapTaskList = new ArrayList<Task>();
    
    List<String> splitInputFiles = dfs.ls(job.getDfsInputPath());
    for(String dfsInputPath: splitInputFiles){
      Task mapTask = new MapTask(dfsInputPath, job.getMapper());
      mapTaskList.add(mapTask);
    }
    
    return mapTaskList;
  }
  
  public boolean addPendingTask(Task task){
    task.setTaskStatus(TaskStatus.PENDING);
    this.pendingTasks.add(task);
    
    return true;
  }
  
  public boolean addRunningTask(Task task){
    task.setTaskStatus(TaskStatus.RUNNING);
    this.runningTasks.add(task);
    return true;
  }
  public boolean addFinishedTask(Task task){
    task.setTaskStatus(TaskStatus.FINISHED);
    this.finishedTasks.add(task);
    return true;
  }
  
  public List<Task> addJob(Job job){
    
    // split job into map tasks. insert them into pending tasks 
    List<Task> mapTaskList = splitJobToMapTaskList(job);
    for(Task mapTask: mapTaskList){
      this.addPendingTask(mapTask);
    }
    
    return mapTaskList;
  }
}
