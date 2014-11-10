package compute.test;

import java.util.List;

import compute.utility.Host;



public abstract class DFS {
  
  public abstract DFSReader getReader(String dfsPath) throws Exception;
  public abstract DFSWriter getWriter(String dfsPath) throws Exception;
  public abstract List<String> ls(String dfsDirPath);
  public abstract Host getHost(String dfsPath, int version);
  
}
