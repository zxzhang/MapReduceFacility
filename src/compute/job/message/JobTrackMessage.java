package compute.job.message;

import java.io.Serializable;
import java.util.List;

import compute.job.Job;
import compute.job.JobStatus;
import compute.task.MapTask;
import compute.task.Task;
import compute.task.TaskStatus;


public class JobTrackMessage implements Serializable {
  JobStatus jobStatus;
  String jobId;
  double maskTaskFinishedRatio;
  double reduceTaskFinishedRatio;
  
  
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
      System.out.println(t);
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
  
  public JobTrackMessage(Job job){
    // calculate ratio of each kind of job 
    this.jobId = job.getJobId();
    this.jobStatus = job.getJobStatus();
    this.maskTaskFinishedRatio = calMaskTaskFinishedRatio(job.mapTasks);
    this.reduceTaskFinishedRatio = 0.0;
  }
  public String toString(){
    return String.format("[%s]-[%s][map: %.2f][reduce: %.2f]", 
        jobId, 
        jobStatus.toString(), 
        this.maskTaskFinishedRatio,
        this.reduceTaskFinishedRatio
    );
  }
}
