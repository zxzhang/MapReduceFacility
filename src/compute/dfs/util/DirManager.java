package compute.dfs.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import compute.configure.AllConfiguration;
import compute.job.TaskTrackerTable;
import compute.job.TaskTrackerTableItem;

public class DirManager {

  private Set<String> distributedDir = null;

  private Map<String, DistributedFile> distributedFile = null;

  AtomicLong fileId = null;

  public DirManager() {
    this.distributedDir = new HashSet<String>();
    this.distributedDir.add("/");
    this.distributedFile = new HashMap<String, DistributedFile>();
    this.fileId = new AtomicLong(0);
  }

  public Set<String> getDir() {
    return this.distributedDir;
  }

  // public Map<String, DistributedFile> getFile() {
  // return this.distributedFile;
  // }

  public DistributedFile getFile(String dir) {
    return this.distributedFile.get(dir);
  }

  public boolean checkFile(String dir) {
    return distributedFile.containsKey(dir);
  }

  public boolean containsDir(String dir) {
    return distributedDir.contains(dir);
  }

  public boolean checkDir(String dir) {
    if (dir == null || dir.length() <= 1) {
      return false;
    }

    String[] path = dir.trim().split("\\/");
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < path.length - 1; i++) {
      if (path[i].length() > 0) {
        sb.append("/").append(path[i]);
      }
    }

    if (sb.length() == 0) {
      sb.append("/");
    }

    if (distributedDir.contains(sb.toString()) && !distributedDir.contains(dir)) {
      return true;
    }

    return false;
  }

  public boolean mkDir(String dir) {
    if (checkDir(dir)) {
      distributedDir.add(dir);
      return true;
    }

    return false;
  }

  public List<String> lsFile(String dir) {
    List<String> dirList = new ArrayList<String>();

    if (dir == null || dir.length() == 0) {
      return dirList;
    }

    // for (String dfsDir : distributedDir) {
    // if (dfsDir.startsWith(dir)
    // && ((dir.equals("/") && dfsDir.lastIndexOf("/") == 0)
    // || dfsDir.length() <= dir.length() || dfsDir.charAt(dir.length()) == '/')) {
    // dirList.add(dfsDir);
    // }
    // }

    for (Entry<String, DistributedFile> entry : distributedFile.entrySet()) {
      String dfsDir = entry.getKey();
      if (dfsDir.startsWith(dir)
              && ((dir.equals("/") && dfsDir.lastIndexOf("/") == 0)
                      || dfsDir.length() <= dir.length() || dfsDir.charAt(dir.length()) == '/')) {
        dirList.add(dfsDir);
      }
    }

    Collections.sort(dirList);
    return dirList;
  }
  
  public List<String> lsDir(String dir) {
    List<String> dirList = new ArrayList<String>();

    if (dir == null || dir.length() == 0) {
      return dirList;
    }

    for (String dfsDir : distributedDir) {
      if (dfsDir.startsWith(dir)
              && ((dir.equals("/") && dfsDir.lastIndexOf("/") == 0)
                      || dfsDir.length() <= dir.length() || dfsDir.charAt(dir.length()) == '/')) {
        dirList.add(dfsDir);
      }
    }

    Collections.sort(dirList);
    return dirList;
  }

  public void printLs(String dir) {
    List<String> dirList = lsDir(dir);
    for (int i = 0; i < dirList.size(); i++) {
      System.out.println(dirList.get(i));
    }
    System.out.println();
  }

  public List<String> lsDir() {
    List<String> dirList = new ArrayList<String>(this.distributedDir);
    Collections.sort(dirList);
    return dirList;
  }

  public DistributedFile getDistributedFile(String dir) {
    return this.distributedFile.get(dir);
  }

  public DistributedFile addDistributedFile(String dfsPath, TaskTrackerTable taskTrackerTable) {
    if (!checkDir(dfsPath)) {
      return null;
    }

    if (checkFile(dfsPath)) {
      return null;
    }

    DistributedFile distributedFile = new DistributedFile(dfsPath, new ArrayList<SlaveLocalFile>());

    List<String> slaveId = new ArrayList<String>();
    for (Entry<String, TaskTrackerTableItem> entry : taskTrackerTable.taskTrackerMap.entrySet()) {
      slaveId.add(entry.getKey());
    }

    Collections.shuffle(slaveId);
    for (int i = 0; i < AllConfiguration.replicate; i++) {
      String filename = "tmp" + (new Long(fileId.incrementAndGet()).toString());
      distributedFile.getSlaveDir().add(
              new SlaveLocalFile(slaveId.get(i % slaveId.size()), "tmp/"
                      + slaveId.get(i % slaveId.size()) + '-' + filename));
    }

    this.distributedFile.put(dfsPath, distributedFile);

    return distributedFile;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();

    sb.append("DIR:\n");
    for (String dir : this.distributedDir) {
      sb.append(dir).append("\n");
    }
    sb.append("FILE:\n");
    for (Entry<String, DistributedFile> entry : this.distributedFile.entrySet()) {
      sb.append(entry.getValue().toString());
    }

    return sb.toString();
  }

}
