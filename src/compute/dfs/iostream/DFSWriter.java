package compute.dfs.iostream;

import compute.job.JobTracker;

public abstract class DFSWriter {

  public abstract void println(String line);

  public abstract void lock(String dfsPath, JobTracker jobTracker) throws Exception;
  
  public abstract void unlock(String dfsPath, JobTracker jobTracker) throws Exception;
}
