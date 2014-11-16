package compute.dfs.iostream;

public abstract class DFSWriter {

  public abstract void println(String line);

  public abstract void lock() throws Exception;
  
  public abstract void unlock() throws Exception;
}
