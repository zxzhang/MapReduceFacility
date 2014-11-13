package compute.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import compute.mapper.Mapper;
import compute.reducer.Reducer;
import compute.task.MapTask;
import compute.task.ReducePreprocessTask;
import compute.task.ReduceTask;


public class Job implements Serializable{
  
  static long maxJobId = 0;
  
  String jobId;
  JobStatus jobStatus;
  String dfsInputPath; 
  List<String> splitInputFiles;
  String dfsOutputPath;
  Class<? extends Mapper> mapper;
  Class<? extends Reducer> reducer;
  public String getJobId(){return this.jobId;}
  public JobStatus getJobStatus(){return this.jobStatus;}
  public void setJobStatus(JobStatus js){this.jobStatus = js;} 
  public String getDfsInputPath(){ return this.dfsInputPath;}
  public String getDfsOutputPath(){return this.dfsOutputPath;}
  public Class<? extends Mapper> getMapper(){return this.mapper;}
  public Class<? extends Reducer> getReducer(){return this.reducer;}
  
  public List<MapTask> mapTasks;
  public List<ReduceTask> reduceTasks;
  public List<ReducePreprocessTask> reducePreprocessTasks;
  
  public void addMapTask(MapTask task){this.mapTasks.add(task);}
  public boolean removeMapTask(MapTask task){return this.mapTasks.remove(task);}
  public void addReducePreprocessTask(ReducePreprocessTask task){this.reducePreprocessTasks.add(task);}
  public boolean removeReducePreprocessTask(ReducePreprocessTask task){return this.reducePreprocessTasks.remove(task);}
  public void addReduceTask(ReduceTask task){this.reduceTasks.add(task);}
  public boolean removeReduceTask(ReduceTask task){return this.reduceTasks.remove(task);}
  
  public String getNewJobId(){
    String newId =  Long.toString(maxJobId);
    maxJobId += 1 ;
    return newId;
  }
  
  public Job( String dfsInputPath, Class<? extends Mapper> mapper, Class<? extends Reducer> reducer, List<String> splitInputFiles){
    
    this.jobId = getNewJobId();
    
    this.jobStatus = JobStatus.PENDING;
    this.dfsInputPath = dfsInputPath;
    this.splitInputFiles = splitInputFiles;
    this.mapper = mapper;
    this.reducer = reducer;
    
    this.mapTasks = new LinkedList<MapTask>();
    this.reducePreprocessTasks = new LinkedList<ReducePreprocessTask>();
    this.reduceTasks = new LinkedList<ReduceTask>();
  }
  
  public boolean equals(Object obj){
    Job job2 =(Job) obj;
    if(job2.jobId.equals(this.jobId)) return true;
    return false;
  }
  public int hashCode(){
    return this.jobId.hashCode();
  }
}
