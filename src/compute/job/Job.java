package compute.job;

import compute.mapper.Mapper;
import compute.reducer.Reducer;


public class Job {
  String jobId;
  JobStatus jobStatus;
  String dfsInputPath; 
  Class<? extends Mapper> mapper;
  Class<? extends Reducer> reducer;
  public String getJobId(){return this.jobId;}
  public JobStatus getJobStatus(){return this.jobStatus;}
  
  public Job(String jobId, String dfsInputPath, Class<? extends Mapper> mapper, Class<? extends Reducer> reducer){
    this.jobId = jobId;
    this.jobStatus = JobStatus.PENDING;
    this.dfsInputPath = dfsInputPath;
    this.mapper = mapper;
    this.reducer = reducer;
  }
}
