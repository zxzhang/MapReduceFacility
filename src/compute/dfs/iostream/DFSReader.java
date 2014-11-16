package compute.dfs.iostream;

import java.io.Serializable;

import compute.job.JobTracker;

public abstract class DFSReader implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 7674275552807637165L;

  public abstract String readLine() throws Exception;
  
  public abstract void close() throws Exception;

  public abstract void lock(String dfsPath, JobTracker jobTracker) throws Exception;

  public abstract void unlock(String dfsPath, JobTracker jobTracker) throws Exception;
}
