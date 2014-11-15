package compute.dfs;

import java.util.List;

import compute.dfs.iostream.DFSReader;
import compute.dfs.iostream.DFSReaderStream;
import compute.dfs.iostream.DFSWriter;
import compute.dfs.iostream.DFSWriterStream;
import compute.dfs.util.DirManager;
import compute.dfs.util.DistributedFile;
import compute.job.TaskTrackerTable;
import compute.utility.Host;

public class MasterDFS extends DFS {

  private DirManager dirManager = null;

  private TaskTrackerTable taskTrackerTable = null;

  public MasterDFS(TaskTrackerTable taskTrackerTable) {
    this.dirManager = new DirManager();
    this.taskTrackerTable = taskTrackerTable;
  }

  @Override
  public DFSReader getReader(String dfsPath) throws Exception {
    if (!dirManager.checkFile(dfsPath)) {
      return null;
    }

    DistributedFile dFile = dirManager.getDistributedFile(dfsPath);
    DFSReader newReader = new DFSReaderStream(dFile, taskTrackerTable);

    return newReader;
  }

  @Override
  public DFSWriter getWriter(String dfsPath) throws Exception {
    if (!dirManager.checkFile(dfsPath)) {
      return null;
    }

    if (!dirManager.checkDir(dfsPath)) {
      return null;
    }

    DistributedFile dFile = dirManager.addDistributedFile(dfsPath, taskTrackerTable);
    DFSWriter newWriter = new DFSWriterStream(dFile, taskTrackerTable);

    return newWriter;
  }

  @Override
  public List<String> ls(String dfsDirPath) {
    return dirManager.lsDir(dfsDirPath);
  }

  @Override
  public Host getHost(String dfsPath, int duplicate) {
    if (!dirManager.checkFile(dfsPath)) {
      return null;
    }

    String slaveId = dirManager.getDistributedFile(dfsPath).getSlaveDir().get(duplicate).getId();
    return taskTrackerTable.taskTrackerMap.get(slaveId).getHost();
  }

  @Override
  public void finishRead() {
  }

  @Override
  public void finishWrite() {
  }

}
