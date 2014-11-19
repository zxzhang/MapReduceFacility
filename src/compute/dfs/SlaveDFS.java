package compute.dfs;

import java.rmi.RemoteException;
import java.util.List;

import compute.dfs.iostream.DFSReader;
import compute.dfs.iostream.DFSWriter;
import compute.job.JobTracker;
import compute.utility.Host;

/*
 * SlaveDFS.java
 * 
 * Author: Zhengxiong Zhang
 * 
 * The implementation of DFS on participant side.
 * 
 * */

public class SlaveDFS extends DFS {

  private JobTracker jobTracker = null;

  public SlaveDFS(JobTracker jobTracker) {
    this.jobTracker = jobTracker;
  }

  @Override
  public DFSReader getReader(String dfsPath) throws Exception {
    return jobTracker.getReader(dfsPath);
  }

  @Override
  public DFSWriter getWriter(String dfsPath) throws Exception {
    return jobTracker.getWriter(dfsPath);
  }

  @Override
  public List<String> ls(String dfsDirPath) {
    try {
      return jobTracker.getLs(dfsDirPath);
    } catch (RemoteException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Host getHost(String dfsPath, int version) {
    try {
      return jobTracker.getHost(dfsPath, version);
    } catch (RemoteException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void addFile(String dfsPath, String localPath) {
    try {
      jobTracker.addFile(dfsPath, localPath);
    } catch (RemoteException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void readLock(String dfsPath) {
    try {
      jobTracker.readLock(dfsPath);
    } catch (RemoteException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void readUnLock(String dfsPath) {
    try {
      jobTracker.readUnLock(dfsPath);
    } catch (RemoteException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void writeLock(String dfsPath) {
    try {
      jobTracker.writeLock(dfsPath);
    } catch (RemoteException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void writeUnLock(String dfsPath) {
    try {
      jobTracker.writeUnLock(dfsPath);
    } catch (RemoteException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
