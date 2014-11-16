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

public class DFSReaderStream extends DFSReader implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 2234842447848013265L;

  private DistributedFile distributedFile = null;

  private TaskTrackerTable taskTrackerTable = null;

  private long[] br = null;

  private TaskTracker[] taskTrackers = null;

  public DFSReaderStream(DistributedFile distributedFile, TaskTrackerTable taskTrackerTable) {
    this.distributedFile = distributedFile;
    this.taskTrackerTable = taskTrackerTable;
    this.br = new long[AllConfiguration.replicate];
    this.taskTrackers = new TaskTracker[AllConfiguration.replicate];
    getRemoteTask();
  }

  private void getRemoteTask() {
    List<SlaveLocalFile> slave = this.distributedFile.getSlaveDir();
    for (int i = 0; i < AllConfiguration.replicate; i++) {
      taskTrackers[i] = taskTrackerTable.get(slave.get(i).getId());
      try {
        br[i] = taskTrackers[i].getBufferReader(slave.get(i).getLocalDir());
      } catch (RemoteException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  @Override
  public String readLine() throws Exception {
    String line = taskTrackers[0].readLine(br[0]);
    return line;
  }

  @Override
  public void lock(String dfsPath, JobTracker jobTracker) throws Exception {
    jobTracker.readLock(dfsPath);
  }

  @Override
  public void unlock(String dfsPath, JobTracker jobTracker) throws Exception {
    jobTracker.readUnLock(dfsPath);
  }

  @Override
  public void close() throws Exception {
    for (int i = 0; i < AllConfiguration.replicate; i++) {
      taskTrackers[i].removeRead(br[i]);
    }
  }

}
