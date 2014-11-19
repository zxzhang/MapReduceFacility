package compute.scheduler;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import compute.configure.AllConfiguration;
import compute.configure.TaskTrackerConfiguration;
import compute.dfs.DFS;
import compute.job.Job;
import compute.job.JobStatus;
import compute.job.JobTable;
import compute.job.TaskTrackerStats;
import compute.job.TaskTrackerTable;
import compute.task.*;
import compute.utility.Host;
import compute.utility.Pair;

/*
 * TaskScheduler.java
 * 
 * Author: San-Chuan Hung
 * 
 * The class schedules the tasks.
 * 
 * */


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
  
  Map<Job, List<Host>> job2reducerHostList; 
  
  Map<Host, List<Task>> host2taskList;
  
  Set<Task> terminatedTasks;
  
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
    
    this.host2taskList = new HashMap<Host, List<Task>>();
    
    this.dfs = dfs;
    this.taskTrackerTable = taskTrackerTable;

    this.job2reducerHostList = new HashMap<Job, List<Host>>();// the index of the host list is the key hash value
    this.terminatedTasks = new HashSet<Task>();
  }
  
  public int getPenndingMapTasksSize(){ return this.pendingMapTasks.size();}
  public int getPenndingReduceTasksSize(){ return this.pendingReduceTasks.size();}
  public int getPenndingReducePreprocessTasksSize(){ return this.pendingReducePreprocessTasks.size();}
  
  public int getFinishedMapTaskSize(){ return this.finishedMapTasks.size();}
  public int getFinishedReducePreprocessTasksSize(){ return this.finishedReducePreprocessTasks.size();}
  public int getFinishedReduceTaskSize(){ return this.finishedReduceTasks.size();}
  
  public void updateHost2TaskList(Task task){
    Host host = task.getHost();
    if(this.host2taskList.containsKey(host)){
      host2taskList.get(host).remove(task);
      host2taskList.get(host).add(task);
    }else{
      host2taskList.put(host, new ArrayList<Task>());
      host2taskList.get(host).add(task);
    }
  }
  
  public boolean addPendingMapTask(MapTask task){
    task.setTaskStatus(TaskStatus.PENDING);
    this.pendingMapTasks.add(task);
    this.updateHost2TaskList(task);
    return true;
  }
  public boolean addRunningMapTask(MapTask task){
    task.setTaskStatus(TaskStatus.RUNNING);
    this.runningMapTasks.add(task);
    this.updateHost2TaskList(task);
    return true;
  }
  public boolean addFinishedMapTask(MapTask task){
    task.setTaskStatus(TaskStatus.FINISHED);
    this.finishedMapTasks.add(task);
    this.updateHost2TaskList(task);
    return true;
  }
  public boolean addPendingReducePreprocessTask(ReducePreprocessTask task){
    task.setTaskStatus(TaskStatus.PENDING);
    this.pendingReducePreprocessTasks.add(task);
    this.updateHost2TaskList(task);
    return true;
  }
  public boolean addRunningReducePreprocessTask(ReducePreprocessTask task){
    task.setTaskStatus(TaskStatus.RUNNING);
    this.runningReducePreprocessTasks.add(task);
    this.updateHost2TaskList(task);
    return true;
  }
  public boolean addFinishedReducePreprocessTask(ReducePreprocessTask task){
    task.setTaskStatus(TaskStatus.FINISHED);
    this.finishedReducePreprocessTasks.add(task);
    this.updateHost2TaskList(task);
    return true;
  }  
  public boolean removeFinishedReducePreprocessTask(ReducePreprocessTask task){
    this.finishedReducePreprocessTasks.remove(task);
    return true;
  }
  public boolean addPendingReduceTask(ReduceTask task){
    task.setTaskStatus(TaskStatus.PENDING);
    this.pendingReduceTasks.add(task);
    this.updateHost2TaskList(task);
    return true;
  }
  public boolean addRunningReduceTask(ReduceTask task){
    task.setTaskStatus(TaskStatus.RUNNING);
    this.runningReduceTasks.add(task);
    this.updateHost2TaskList(task);
    return true;
  }
  public boolean addFinishedReduceTask(ReduceTask task){
    task.setTaskStatus(TaskStatus.FINISHED);
    this.finishedReduceTasks.add(task);
    this.updateHost2TaskList(task);
    return true;
  }    
  /****************************************************************************/

  public void removeAllReduceHosts(List<Host> hostList){
    for(Host host : hostList){
      removeReducerHost(host);
    }
  }
  
  public void removeReducerHost(Host host){
    for(List<Host> hostList: job2reducerHostList.values()){
      hostList.remove(host);
    }
  }
  
  /****************************************************************************/
  
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
    // set job start to run
    job.setJobStatus(JobStatus.RUNNING);
    // split job into map tasks. insert them into pending tasks 
    List<MapTask> mapTaskList = splitJobToMapTaskList(job);
    for(MapTask mapTask: mapTaskList){
      this.addPendingMapTask(mapTask);
    }
    return mapTaskList;
  }
  
  public boolean finishMapTask(MapTask task){
    this.runningMapTasks.remove(task);
    this.addFinishedMapTask(task);
    return true;
  }
  
  public boolean finishReducePreprocessTask(ReducePreprocessTask task){
    this.runningReducePreprocessTasks.remove(task);
    this.addFinishedReducePreprocessTask(task);
    return true;
  }
  
  public boolean finishReduceTask(ReduceTask task){
    this.runningReduceTasks.remove(task);
    this.addFinishedReduceTask(task);
    return true;
  }
  
  /****************************************************************************/
  
  public boolean schedulePendingMapTask() {
    Iterator<MapTask> pendingMapTasks = this.pendingMapTasks.iterator();
        
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
  
  public boolean scheduleFinishedMapTask(){
    Iterator<MapTask> finishedTasks = this.finishedMapTasks.iterator();
    while(finishedTasks.hasNext()){
      MapTask task = finishedTasks.next();
      
      // generate R pending ReducePreprocess tasks
      for(int reducerNum = 0; reducerNum < AllConfiguration.numOfReducer; reducerNum++){
        ReducePreprocessTask reducePreprocessTask = new ReducePreprocessTask(reducerNum, task);
        this.addPendingReducePreprocessTask(reducePreprocessTask);
      }
      
      finishedTasks.remove();
    }
    return true;
  }
  
  /****************************************************************************/
  public boolean schedulePendingReducePreprocessTask(){
    Iterator<ReducePreprocessTask> pendingTaskIter = this.pendingReducePreprocessTasks.iterator();
    while(pendingTaskIter.hasNext()){
      ReducePreprocessTask task = pendingTaskIter.next();
      
      // find host without map tasks  
      Host reducerHost = taskTrackerTable.getAvaliableReducerHost();
      if(reducerHost == null){
        continue;
      }
      
      TaskTracker taskTracker = taskTrackerTable.get(reducerHost);
      try {
        if(taskTracker.assignReducePreprocessTask(task)){
          pendingTaskIter.remove();
          this.addRunningReducePreprocessTask(task);
          //update job2reducerHostList
          this.updateJob2ReducerHostList(task.getJob(), task.getReducerNum(), reducerHost);
        }
      } catch (RemoteException e) {
        System.out.println("Cannot assign ReducePreprocessTask: "+ taskTracker);
        e.printStackTrace();
      }
    }
    return true;
  }
  
  public boolean scheduleFinishedReducePreprocessTask(){
    // for each job , if some one has collected all map_{$i} file -> run reduce
    Map<Pair<Job, Integer>, List<ReducePreprocessTask>> jobReducerNum2ReducePreprocessTasks = 
        new HashMap<Pair<Job,Integer>, List<ReducePreprocessTask>>();
    for(ReducePreprocessTask task: finishedReducePreprocessTasks){
      Integer reducerNum = task.getReducerNum();
      Pair<Job, Integer> pair = new Pair<Job, Integer>(task.getJob(), reducerNum);

      if(!jobReducerNum2ReducePreprocessTasks.containsKey(pair)){
        jobReducerNum2ReducePreprocessTasks.put(pair, new ArrayList<ReducePreprocessTask>());
      }
      jobReducerNum2ReducePreprocessTasks.get(pair).add(task);
    }
    
    // check if reduce task can start or not
    for(Pair<Job, Integer> pair: jobReducerNum2ReducePreprocessTasks.keySet()){
      Job job = pair.getFirst();
      Integer reducerNum = pair.getSecond();
      List<ReducePreprocessTask> allTasks = jobReducerNum2ReducePreprocessTasks.get(pair);

      if(allTasks.size() == job.mapTasks.size()){ // all reducePreprocess tasks are finished
        
        // prepare localInputPaths
        List<String> localInputPaths = new ArrayList<String>();
        for(ReducePreprocessTask reducePreprocessTask : allTasks){
          localInputPaths.add(reducePreprocessTask.getLocalSortedOutputFilePath());
        }
                
        // add into ReduceTask
        ReduceTask reduceTask = new ReduceTask(reducerNum, job.getDfsOutputPath(), job.getReducer(), localInputPaths, job);
        this.addPendingReduceTask(reduceTask);
        
        // remove reducePreprocessTask
        for(ReducePreprocessTask reducePreprocessTask: allTasks){
          removeFinishedReducePreprocessTask(reducePreprocessTask);
        }

      }
     
    }
    return true;
  }

  /****************************************************************************/

  public boolean schedulePendingReduceTask(){
    Iterator<ReduceTask> pendingTaskIter = this.pendingReduceTasks.iterator();
    while(pendingTaskIter.hasNext()){
      ReduceTask task = pendingTaskIter.next();
      
      // find host without map tasks  
      Host reducerHost = taskTrackerTable.getAvaliableReducerHost();
      if(reducerHost == null){
        // cannot find the corresponding Host -> drop it
        System.out.println("Error: cannot find host for reduce task: "+task);
        pendingTaskIter.remove();
        continue;
      }
      
      TaskTracker taskTracker = taskTrackerTable.get(reducerHost);
      try {
        if(taskTracker.assignReduceTask(task)){
          pendingTaskIter.remove();
          this.addRunningReduceTask(task);
        }
      } catch (RemoteException e) {
        System.out.println("Cannot assign ReduceTask: "+ taskTracker);
        e.printStackTrace();
      }
    }
    return true;
  }
  
  public List<Job> checkFinishedJobs(){
    List<Job> finishedJobs = new ArrayList<Job>();
    
    Iterator<ReduceTask> finishedTaskIter = this.finishedReduceTasks.iterator();
    
    Map<Job, List<ReduceTask>> job2tasks = new HashMap<Job, List<ReduceTask>>();
    
    // check whether the job is finished or not.
    while(finishedTaskIter.hasNext()){
      ReduceTask task = finishedTaskIter.next();
      if(!job2tasks.containsKey(task.getJob())){
        job2tasks.put(task.getJob(), new ArrayList<ReduceTask>());
      }
      job2tasks.get(task.getJob()).add(task);
    }
    
    
    for(Job job: job2tasks.keySet()){
      // the job is done
      if(job2tasks.get(job).size() == AllConfiguration.numOfReducer){
        // 1. change job status into completed
        job.setJobStatus(JobStatus.COMPLETED);
        finishedJobs.add(job);
        // 2. remove all finished reduce tasks
        this.finishedReduceTasks.removeAll(job2tasks.get(job));
      }
    }
    
    return finishedJobs;
  }

  /****************************************************************************/

  public void updateJob2ReducerHostList(Job job, int reducerNum, Host host){
    if(!job2reducerHostList.containsKey(job)){
      Vector<Host> list = new Vector<Host>(AllConfiguration.numOfReducer);
      list.setSize(AllConfiguration.numOfReducer);
      job2reducerHostList.put(job, list);
    }
    
    this.job2reducerHostList.get(job).add(reducerNum, host);
  }
  
  public Host getHostByJobReducerNum(Job job, int reducerNum){
    if(!job2reducerHostList.containsKey(job)){
      List<Host> hosts = job2reducerHostList.get(job);
      return hosts.get(reducerNum);
    }
    return null;
  }
  
  /****************************************************************************/

  public void reschedule(List<Host> deadHosts, List<Task> toBeDeletedTasks, List<Task> toBeAddedTasks){

    for( Host deadHost : deadHosts){

      List<Task> deadHostTasks = this.host2taskList.get(deadHost);
      if(deadHostTasks == null){ continue; }
      // regenerate it's task into pending queue
      for(Task deadHostTask : deadHostTasks){
        Task reviveTask =null;
        
        if(deadHostTask.getTaskType() == TaskType.MAP){
          MapTask originTask = (MapTask) deadHostTask;
          reviveTask = (Task) new MapTask(originTask.getDfsInputPath(), originTask.getMapperClass());
          reviveTask.setJob(originTask.getJob());
        }else if(deadHostTask.getTaskType() == TaskType.REDUCEPREPROCESS){
          ReducePreprocessTask originTask = (ReducePreprocessTask) deadHostTask;
          if(!deadHosts.contains(originTask.getDataSourceHost()) ){
            reviveTask = (Task) new ReducePreprocessTask(originTask);
          }
        }
//        else if(deadHostTask.getTaskType() == TaskType.REDUCE){
//          ReduceTask originTask = (ReduceTask) deadHostTask;
//          reviveTask = (Task) new ReduceTask(originTask);
//        }
        if(reviveTask != null){
          reviveTask.setTaskStatus(TaskStatus.PENDING);
          toBeAddedTasks.add(reviveTask);
        }
        toBeDeletedTasks.add(deadHostTask);
      }
      
      // check whether it contains map intermediate files 
      // == check-> check it has finished map task
      List<MapTask> finishedMapTasks = new ArrayList<MapTask>();
      for(Task deadHostTask: deadHostTasks){
        if(deadHostTask.getTaskStatus() == TaskStatus.FINISHED &&  
            deadHostTask.getTaskType() == TaskType.MAP){
          
          finishedMapTasks.add((MapTask) deadHostTask);
        }
      }
      
      // if yes -> kill all dependent reduce pre-fetch tasks and reduce tasks
      if(finishedMapTasks.size() > 0){
        Iterator<ReducePreprocessTask> pendingReducePreprocessTaskIter = this.pendingReducePreprocessTasks.iterator();
        while(pendingReducePreprocessTaskIter.hasNext()){
          ReducePreprocessTask task = pendingReducePreprocessTaskIter.next();
          if(task.getDataSourceHost().equals(deadHost)){
            pendingReducePreprocessTaskIter.remove();
            this.terminatedTasks.add(task);
          }
        }
        Iterator<ReducePreprocessTask> runningReducePreprocessTaskIter = this.runningReducePreprocessTasks.iterator();
        while(runningReducePreprocessTaskIter.hasNext()){
          ReducePreprocessTask task = runningReducePreprocessTaskIter.next();
          if(task.getDataSourceHost().equals(deadHost)){
            runningReducePreprocessTaskIter.remove();
            this.terminatedTasks.add(task);
          }
        }
      }
    }
  }
  
  /****************************************************************************/
  
  public String toString(){
    return String.format(
        "map\np: %s\nr: %s\nf: %s\n"+ "reduce_pre\np: %s\nr: %s\nf: %s\n" +"reduce\np: %s\nr: %s\nf: %s\n", 
        this.pendingMapTasks.toString(), 
        this.runningMapTasks.toString(), 
        this.finishedMapTasks.toString(),
        this.pendingReducePreprocessTasks.toString(),
        this.runningReducePreprocessTasks.toString(),
        this.finishedReducePreprocessTasks.toString(),
        this.pendingReduceTasks.toString(),
        this.runningReduceTasks.toString(),
        this.finishedReduceTasks.toString()
    );
  }
}
