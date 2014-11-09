package compute.job.message;

import java.io.Serializable;

import compute.job.Job;
import compute.job.JobStatus;


public class JobTrackMessage implements Serializable {
  JobStatus jobStatus;
  String jobId;
  public JobStatus getJobStatus(){
    return jobStatus;
  }
  public JobTrackMessage(JobStatus jobStatus, String jobId){
    this.jobStatus = jobStatus;
    this.jobId = jobId;
  }
  public JobTrackMessage(Job job){
    this.jobId = job.getJobId();
    this.jobStatus = job.getJobStatus();
  }
  public String toString(){
    return String.format("[%s]-[%s]", jobId, jobStatus.toString());
  }
}
