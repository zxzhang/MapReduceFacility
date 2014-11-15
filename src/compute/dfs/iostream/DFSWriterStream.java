package compute.dfs.iostream;

import java.io.PrintStream;
import java.util.List;

import compute.configure.AllConfiguration;
import compute.dfs.util.DistributedFile;
import compute.dfs.util.SlaveLocalFile;
import compute.job.TaskTrackerTable;
import compute.task.TaskTracker;

public class DFSWriterStream extends DFSWriter {

  private DistributedFile distributedFile = null;

  private TaskTrackerTable taskTrackerTable = null;

  private PrintStream[] ps = null;

  private TaskTracker[] taskTrackers = null;

  public DFSWriterStream(DistributedFile distributedFile, TaskTrackerTable taskTrackerTable) {
    this.distributedFile = distributedFile;
    this.taskTrackerTable = taskTrackerTable;
    this.ps = new PrintStream[AllConfiguration.replicate];
    this.taskTrackers = new TaskTracker[AllConfiguration.replicate];
    getRemoteTask();
  }

  private void getRemoteTask() {
    List<SlaveLocalFile> slave = this.distributedFile.getSlaveDir();
    for (int i = 0; i < AllConfiguration.replicate; i++) {
      taskTrackers[i] = taskTrackerTable.get(slave.get(i).getId());
      ps[i] = taskTrackers[i].getPrintStream(slave.get(i).getLocalDir());
    }

  }

  @Override
  public void println(String line) {
    for (int i = 0; i < AllConfiguration.replicate; i++) {
      taskTrackers[i].printLine(ps[i], line);
    }
    distributedFile.addSize(1);
  }

}
