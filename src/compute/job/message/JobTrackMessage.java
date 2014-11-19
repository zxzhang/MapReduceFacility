package compute.job.message;

import java.io.Serializable;
import java.util.List;

import compute.job.Job;
import compute.job.JobStatus;
import compute.task.MapTask;
import compute.task.ReducePreprocessTask;
import compute.task.ReduceTask;
import compute.task.Task;
import compute.task.TaskStatus;

/*
 * JobTrackerMessage.java
 * 
 * Author: San-Chuan Hung
 * 
 * JobClient get job progresss information with this class. 
 * 
 * */

public class JobTrackMessage implements Serializable {
  JobStatus jobStatus;
  String jobId;
  double maskTaskFinishedRatio;
  double reducePreprocessFinishedRatio;
  double reduceTaskFinishedRatio;
  String dfsOutputPath;
  
  public String getDfsOutputPath(){
    return dfsOutputPath;
  }
  public JobStatus getJobStatus(){
    return jobStatus;
  }
  public JobTrackMessage(JobStatus jobStatus, String jobId){
    this.jobStatus = jobStatus;
    this.jobId = jobId;
  }
  
  private double calFinishedTask(List<? extends Task> tasks){
    double num = 0;
    for(Task t: tasks){
      if(t.getTaskStatus() == TaskStatus.FINISHED){
        num +=1.0;
      }
    }
    return num;
  }
  
  public double calMaskTaskFinishedRatio(List<MapTask> mapTasks){
    if(mapTasks.size() == 0){return 0.0;}
    
    return 100.0 * (calFinishedTask(mapTasks) / ((double)mapTasks.size()));
  }
  
  public double calReducePreprocessTaskFinishedRatio(List<ReducePreprocessTask> reducePreprocessTasks){
    if(reducePreprocessTasks.size() == 0){return 0.0;}
  
    return 100.0 * (calFinishedTask(reducePreprocessTasks)/ ((double) reducePreprocessTasks.size()));
  }
  
  public double calReduceTaskFinishedRatio(List<ReduceTask> reduceTasks){
    if(reduceTasks.size() == 0){return 0.0;}
    
    return 100.0 * (calFinishedTask(reduceTasks)/ ((double) reduceTasks.size()));
  }
  
  public JobTrackMessage(Job job){
    // calculate ratio of each kind of job 
    this.jobId = job.getJobId();
    this.jobStatus = job.getJobStatus();
    this.maskTaskFinishedRatio = calMaskTaskFinishedRatio(job.mapTasks);
    this.reducePreprocessFinishedRatio = calReducePreprocessTaskFinishedRatio(job.reducePreprocessTasks);
    this.reduceTaskFinishedRatio = calReduceTaskFinishedRatio(job.reduceTasks);
    this.dfsOutputPath = job.getDfsOutputPath();
  }
  public String toString(){
    return String.format("[%s]-[%s][map: %.0f%%][sort: %.0f%%][reduce: %.0f%%]", 
        jobId, 
        jobStatus.toString(), 
        this.maskTaskFinishedRatio,
        this.reducePreprocessFinishedRatio,
        this.reduceTaskFinishedRatio
    );
  }
}
