package compute.dfs.iostream;

import java.io.BufferedReader;
import java.util.List;

import compute.configure.AllConfiguration;
import compute.dfs.util.DistributedFile;
import compute.dfs.util.SlaveLocalFile;
import compute.job.TaskTrackerTable;
import compute.task.TaskTracker;

public class DFSReaderStream extends DFSReader {

  private DistributedFile distributedFile = null;

  private TaskTrackerTable taskTrackerTable = null;

  private BufferedReader[] br = null;

  private TaskTracker[] taskTrackers = null;

  public DFSReaderStream(DistributedFile distributedFile, TaskTrackerTable taskTrackerTable) {
    this.distributedFile = distributedFile;
    this.taskTrackerTable = taskTrackerTable;
    this.br = new BufferedReader[AllConfiguration.replicate];
    this.taskTrackers = new TaskTracker[AllConfiguration.replicate];
    getRemoteTask();
  }

  private void getRemoteTask() {
    List<SlaveLocalFile> slave = this.distributedFile.getSlaveDir();
    for (int i = 0; i < AllConfiguration.replicate; i++) {
      taskTrackers[i] = taskTrackerTable.get(slave.get(i).getId());
      br[i] = taskTrackers[i].getBufferReader(slave.get(i).getLocalDir());
    }
  }

  @Override
  public String readLine() throws Exception {
    String line = taskTrackers[0].readLine(br[0]);
    return line;
  }

}
