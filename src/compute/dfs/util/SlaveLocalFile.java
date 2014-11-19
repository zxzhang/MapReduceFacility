package compute.dfs.util;

import java.io.Serializable;

/*
 * SlaveLocalFile.java
 * 
 * Author: Zhengxiong Zhang
 * 
 * */

public class SlaveLocalFile implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = -6821425636482142241L;

  private String id = null;

  private String localDir = null;

  public SlaveLocalFile(String id, String localDir) {
    this.id = id;
    this.localDir = localDir;
  }

  public String getId() {
    return this.id;
  }

  public String getLocalDir() {
    return this.localDir;
  }
}
