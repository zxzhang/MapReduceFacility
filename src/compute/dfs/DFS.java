package compute.dfs;

import java.util.List;

import compute.dfs.iostream.DFSReader;
import compute.dfs.iostream.DFSWriter;
import compute.utility.Host;

public abstract class DFS {

  public abstract DFSReader getReader(String dfsPath) throws Exception;

  public abstract DFSWriter getWriter(String dfsPath) throws Exception;

  public abstract List<String> ls(String dfsDirPath);

  public abstract Host getHost(String dfsPath, int version);

  public abstract void finishRead();
  
  public abstract void finishWrite();
}
