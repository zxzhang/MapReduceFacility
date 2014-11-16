package compute.dfs;

import java.util.List;

import compute.dfs.iostream.DFSReader;
import compute.dfs.iostream.DFSWriter;
import compute.job.JobTracker;
import compute.utility.Host;

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
    return jobTracker.getLs(dfsDirPath);
  }

  @Override
  public Host getHost(String dfsPath, int version) {
    return jobTracker.getHost(dfsPath, version);
  }

  @Override
  public void addFile(String dfsPath, String localPath) {
    jobTracker.addFile(dfsPath, localPath);
  }

  @Override
  public void readLock(String dfsPath) {
    jobTracker.readLock(dfsPath);
  }

  @Override
  public void readUnLock(String dfsPath) {
    jobTracker.readUnLock(dfsPath);
  }

  @Override
  public void writeLock(String dfsPath) {
    jobTracker.writeLock(dfsPath);
  }

  @Override
  public void writeUnLock(String dfsPath) {
    jobTracker.writeUnLock(dfsPath);
  }
}
