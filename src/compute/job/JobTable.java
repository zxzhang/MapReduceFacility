package compute.job;

/*
 * JobTable.java
 * 
 * Author: San-Chuan Hung
 * 
 * This class manage jobs in coordinator. 
 * 
 * */
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import compute.mapper.Mapper;
import compute.reducer.Reducer;
import compute.task.MapTask;
import compute.task.ReducePreprocessTask;
import compute.task.ReduceTask;
import compute.task.Task;
import compute.task.TaskType;

class IDGenerator{
  private SecureRandom random = new SecureRandom();
  
  public String nextStringID(){
    return new BigInteger(130, random).toString(8);
  }
}

public class JobTable {
  Map<String, Job> tableMap;
  IDGenerator idGenerator;
  
  
  public JobTable(){
    tableMap = new HashMap<String, Job>();
    idGenerator = new IDGenerator();
  }
  
  public boolean removeJob(String jobId){
    return tableMap.remove(jobId) != null;
  }
  
  public Job addJob(String dfsInputPath, String dfsOutputPath, Class< ? extends Mapper> mapper, Class<? extends Reducer> reducer, List<String> splitInputFiles){
    // get job id
    String jobId;
    while(true){
      jobId = idGenerator.nextStringID();
      if(tableMap.containsKey(jobId)){
        jobId = idGenerator.nextStringID();
      }else{
        break;
      }
    }
    
    Job job = new Job(dfsInputPath, dfsOutputPath, mapper, reducer, splitInputFiles);
    // insert into tableMap
    tableMap.put(job.getJobId(), job); 
    
    return job;
  }
  
  public void updateMapTask(String jobId, MapTask task){
    Job job = this.get(jobId);
    job.removeMapTask(task);
    job.addMapTask(task);
  }
  
  public void deleteMapTask(String jobId, MapTask task){
    Job job = this.get(jobId);
    job.removeMapTask(task);
  }
  
  public void updateReducePreprocessTask(String jobId, ReducePreprocessTask task){
    Job job = this.get(jobId);
    job.removeReducePreprocessTask(task);
    job.addReducePreprocessTask(task);
  }
  
  public void deleteReducePreprocessTask(String jobId, ReducePreprocessTask task){
    Job job = this.get(jobId);
    job.removeReducePreprocessTask(task);
  }
  
  public void updateReduceTask(String jobId, ReduceTask task){
    Job job = this.get(jobId);
    job.removeReduceTask(task);
    job.addReduceTask(task);
  }
  
  public void deleteReduceTask(String jobId, ReduceTask task){
    Job job = this.get(jobId);
    job.removeReduceTask(task);
  }
  
  public void deleteTasks(List<Task> tasks){
    for(Task task: tasks){
      String jobId = task.getJob().getJobId();
      if(task.getTaskType() == TaskType.MAP){
        this.deleteMapTask(jobId, (MapTask) task);
      }else if(task.getTaskType() == TaskType.REDUCEPREPROCESS){
        this.deleteReducePreprocessTask(jobId, (ReducePreprocessTask) task);
      }else if(task.getTaskType() == TaskType.REDUCE){
        this.deleteReduceTask(jobId, (ReduceTask) task);
      }
    }
  }
  
  public void addTasks(List<Task> tasks){
    for(Task task: tasks){
      String jobId = task.getJob().getJobId();
      if(task.getTaskType() == TaskType.MAP){
        this.updateMapTask(jobId, (MapTask) task);
      }else if(task.getTaskType() == TaskType.REDUCEPREPROCESS){
        this.updateReducePreprocessTask(jobId, (ReducePreprocessTask) task);
      }else if(task.getTaskType() == TaskType.REDUCE){
        this.updateReduceTask(jobId, (ReduceTask) task);
      }
    }
  }
 
  
  public boolean updateJobStatus(String jobId, JobStatus status){
    Job job = tableMap.get(jobId);
    if(job == null){return false;}
    
    job.setJobStatus(status);
    return true;
  }
  
  public Job get(String jobId){
     return tableMap.get(jobId);
  }
}
