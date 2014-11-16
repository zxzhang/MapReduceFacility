package compute.dfs.iostream;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.List;

import compute.configure.AllConfiguration;
import compute.dfs.util.DistributedFile;
import compute.dfs.util.SlaveLocalFile;
import compute.job.JobTracker;
import compute.job.TaskTrackerTable;
import compute.task.TaskTracker;

public class DFSWriterStream extends DFSWriter implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 5233608056016372838L;

  private DistributedFile distributedFile = null;

  private TaskTrackerTable taskTrackerTable = null;

  private long[] ps = null;

  private TaskTracker[] taskTrackers = null;

  public DFSWriterStream(DistributedFile distributedFile, TaskTrackerTable taskTrackerTable) {
    this.distributedFile = distributedFile;
    this.taskTrackerTable = taskTrackerTable;
    this.ps = new long[AllConfiguration.replicate];
    this.taskTrackers = new TaskTracker[AllConfiguration.replicate];
    getRemoteTask();
  }

  private void getRemoteTask() {
    List<SlaveLocalFile> slave = this.distributedFile.getSlaveDir();

    for (int i = 0; i < AllConfiguration.replicate; i++) {
      taskTrackers[i] = taskTrackerTable.get(slave.get(i).getId());
      try {
        ps[i] = taskTrackers[i].getPrintStream(slave.get(i).getLocalDir());
      } catch (RemoteException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  @Override
  public void println(String line) throws Exception {
    for (int i = 0; i < AllConfiguration.replicate; i++) {
      try {
        taskTrackers[i].printLine(ps[i], line);
      } catch (RemoteException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    distributedFile.addSize(1);
  }

  @Override
  public void lock(String dfsPath, JobTracker jobTracker) throws Exception {
    jobTracker.writeLock(dfsPath);
  }

  @Override
  public void unlock(String dfsPath, JobTracker jobTracker) throws Exception {
    jobTracker.writeUnLock(dfsPath);
  }

  @Override
  public void close() throws Exception {
    for (int i = 0; i < AllConfiguration.replicate; i++) {
      taskTrackers[i].removeWrite(ps[i]);
    }
  }

}
