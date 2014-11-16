package compute.dfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import compute.configure.AllConfiguration;
import compute.dfs.iostream.DFSReader;
import compute.dfs.iostream.DFSReaderStream;
import compute.dfs.iostream.DFSWriter;
import compute.dfs.iostream.DFSWriterStream;
import compute.dfs.util.DirManager;
import compute.dfs.util.DistributedFile;
import compute.dfs.util.ReadWriteLock;
import compute.job.TaskTrackerTable;
import compute.utility.Host;

public class MasterDFS extends DFS {

  private DirManager dirManager = null;

  private TaskTrackerTable taskTrackerTable = null;

  // private ReadWriteLock readWriteLock = null;

  public MasterDFS(TaskTrackerTable taskTrackerTable) {
    this.dirManager = new DirManager();
    this.taskTrackerTable = taskTrackerTable;
    // this.readWriteLock = ReadWriteLock.getInstance();
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
    ReadWriteLock.getInstance().readUnlock();
  }

  @Override
  public void finishWrite() {
    ReadWriteLock.getInstance().writeUnlock();
  }

  @Override
  public void addFile(String dfsPath, String localPath) {

    FileReader reader;

    try {
      reader = new FileReader(localPath);
    } catch (FileNotFoundException e) {
      System.out.println(e.getMessage());
      return;
    }

    BufferedReader bf = new BufferedReader(reader);
    int numOfLine = 0; // AllConfiguration.blockFileLength;

    String line = null;
    DFSWriter writer = null;

    try {
      while ((line = bf.readLine()) != null) {
        
        if (numOfLine == 0) {
          if (writer != null) {
            finishWrite();
          }
          
          writer = getWriter(dfsPath);
          numOfLine = AllConfiguration.blockFileLength;
        }
        
        writer.println(line.trim());
        
        numOfLine--;
      }
      
      finishWrite();
    } catch (IOException e) {
      System.out.println(e.getMessage());
      return;
    } catch (Exception e) {
      System.out.println(e.getMessage());
      return;
    }

  }

}
