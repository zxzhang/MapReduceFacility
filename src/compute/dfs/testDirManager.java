package compute.dfs;

import compute.dfs.util.DirManager;

public class testDirManager {
  public static void main(String[] args) {
    DirManager obj = new DirManager();
    obj.printLs("/");
    obj.mkDir("/abc");
    obj.printLs("/");
    
    obj.mkDir("/ab");
    obj.printLs("/");
    
    obj.mkDir("/ab/abc");
    obj.printLs("/ab");
    obj.printLs("/");
  }
}
