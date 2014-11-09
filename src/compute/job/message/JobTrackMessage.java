package compute.job.message;

import compute.job.Job;
import compute.job.JobStatus;


public class JobTrackMessage {
  JobStatus jobStatus;
  String jobId;
  public JobTrackMessage(JobStatus jobStatus, String jobId){
    this.jobStatus = jobStatus;
    this.jobId = jobId;
  }
  public JobTrackMessage(Job job){
    this.jobId = job.getJobId();
    this.jobStatus = job.getJobStatus();
  }
}
