package compute.dfs.iostream;

import compute.job.JobTracker;

public abstract class DFSReader {
  
  public abstract String readLine() throws Exception;
  
  public abstract void lock(String dfsPath, JobTracker jobTracker) throws Exception;
  
  public abstract void unlock(String dfsPath, JobTracker jobTracker) throws Exception;
}
