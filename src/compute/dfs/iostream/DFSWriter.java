package compute.dfs.iostream;

import java.io.Serializable;

import compute.job.JobTracker;

/*
 * DFSWriter.java
 * 
 * Author: Zhengxiong Zhang
 * 
 * The abstract class of DFS Writer.
 * 
 * */


public abstract class DFSWriter implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 5387151202554744379L;

  public abstract void println(String line) throws Exception;;

  public abstract void close() throws Exception;

  public abstract void lock(String dfsPath, JobTracker jobTracker) throws Exception;

  public abstract void unlock(String dfsPath, JobTracker jobTracker) throws Exception;
}
