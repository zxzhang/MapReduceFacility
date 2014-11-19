package compute.dfs;

import java.util.List;

/*
 * DFS.java
 * 
 * Author:  Zhengxiong Zhang
 * 
 * This is the interface for the DFS file system.
 * 
 * */

import compute.dfs.iostream.DFSReader;
import compute.dfs.iostream.DFSWriter;
import compute.utility.Host;

public abstract class DFS {

  public abstract DFSReader getReader(String dfsPath) throws Exception;

  public abstract DFSWriter getWriter(String dfsPath) throws Exception;

  public abstract List<String> ls(String dfsDirPath);

  public abstract Host getHost(String dfsPath, int version);

  public abstract void addFile(String dfsPath, String localPath);

  public abstract void readLock(String dfsPath);

  public abstract void readUnLock(String dfsPath);

  public abstract void writeLock(String dfsPath);

  public abstract void writeUnLock(String dfsPath);
}
