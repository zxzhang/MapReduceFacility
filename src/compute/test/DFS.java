package compute.test;

import java.util.List;



public abstract class DFS {
  
  
  public abstract DFSReader getReader(String dfsPath) throws Exception;
  public abstract DFSWriter getWriter(String dfsPath) throws Exception;
  public abstract List<String> ls(String dfsDirPath);
  
}
