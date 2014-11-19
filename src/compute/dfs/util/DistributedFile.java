package compute.dfs.util;

import java.io.Serializable;
import java.util.List;

/*
 * DistributedFiles.java
 * 
 * Author: Zhengxiong Zhang
 * 
 * Distributed file class.
 * 
 * */

public class DistributedFile implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 869174308136764079L;

  private String dir = null;

  private List<SlaveLocalFile> slaveDir = null;

  private int size = 0;

  private ReadWriteLock lock = null;

  public DistributedFile(String dir, List<SlaveLocalFile> slaveDir) {
    this.dir = dir;
    this.slaveDir = slaveDir;
    this.size = 0;
    this.lock = new ReadWriteLock();
  }

  public String getDir() {
    return this.dir;
  }

  public void lockRead() throws Exception {
    this.lock.readLock();
  }

  public void unlockRead() throws Exception {
    this.lock.readUnlock();
  }

  public void lockWrite() throws Exception {
    this.lock.writeLock();
  }

  public void unlockWrite() throws Exception {
    this.lock.writeUnlock();
  }

  // @Override
  // public int hashCode() {
  // return this.distributedDir.hashCode() * 33 + this.fileName.hashCode();
  // }
  //
  // @Override
  // public boolean equals(Object obj) {
  // if (this == obj) {
  // return true;
  // }
  //
  // if (obj == null || this.getClass() != obj.getClass()) {
  // return false;
  // }
  //
  // DistributedFile distributedFile = (DistributedFile) obj;
  //
  // if (distributedFile.distributedDir.equals(this.distributedDir)
  // && distributedFile.fileName.equals(this.fileName)) {
  // return true;
  // }
  //
  // return false;
  // }
  //
  // public String getDistributedDir() {
  // return this.distributedDir;
  // }
  //
  // public String getFileName() {
  // return this.fileName;
  // }

  public List<SlaveLocalFile> getSlaveDir() {
    return this.slaveDir;
  }

  public int size() {
    return this.size;
  }

  public void addSize(int size) {
    this.size += size;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("DIR: ").append(dir).append("\n");
    for (int i = 0; i < this.slaveDir.size(); i++) {
      sb.append("File").append(i).append(": ").append(this.slaveDir.get(i).getId()).append('\t')
              .append(this.slaveDir.get(i).getLocalDir()).append('\n');
    }

    return sb.toString();
  }
}
