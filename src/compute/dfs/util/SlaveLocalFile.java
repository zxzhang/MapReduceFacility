package compute.dfs.util;

public class SlaveLocalFile {

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
