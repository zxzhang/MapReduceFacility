package compute.job;

import java.rmi.Remote;

import compute.job.message.JobTrackMessage;




public interface JobTracker extends  Remote {

  public String submitJob(String dfsInputPath, Class<? extends Mapper> mapper, Class<? extends Reducer> reducer);

  public JobTrackMessage trackJob(String jobId);
  
  
}
