package compute.job;

import java.io.Serializable;
import java.util.List;

import compute.mapper.Mapper;
import compute.reducer.Reducer;


public class Job implements Serializable{
  String jobId;
  JobStatus jobStatus;
  String dfsInputPath; 
  List<String> splitInputFiles;
  String dfsOutputPath;
  Class<? extends Mapper> mapper;
  Class<? extends Reducer> reducer;
  public String getJobId(){return this.jobId;}
  public JobStatus getJobStatus(){return this.jobStatus;}
  public String getDfsInputPath(){ return this.dfsInputPath;}
  public Class<? extends Mapper> getMapper(){return this.mapper;}
  
  public Job(String jobId, String dfsInputPath, Class<? extends Mapper> mapper, Class<? extends Reducer> reducer, List<String> splitInputFiles){
    this.jobId = jobId;
    this.jobStatus = JobStatus.PENDING;
    this.dfsInputPath = dfsInputPath;
    this.splitInputFiles = splitInputFiles;
    this.mapper = mapper;
    this.reducer = reducer;
  }
}
