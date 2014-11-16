package compute.dfs.iostream;

public abstract class DFSReader {
  
  public abstract String readLine() throws Exception;
  
  public abstract void lock() throws Exception;
  
  public abstract void unlock() throws Exception;
}
