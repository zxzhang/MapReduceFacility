package compute.dfs.util;

import java.util.List;

public class DistributedFile {

  private List<SlaveLocalFile> slaveDir = null;

  private int size = 0;

  public DistributedFile(List<SlaveLocalFile> slaveDir) {
    this.slaveDir = slaveDir;
    this.size = 0;
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
}
