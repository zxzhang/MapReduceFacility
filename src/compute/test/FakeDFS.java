package compute.test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import compute.utility.Host;



public class FakeDFS extends DFS {
  String prefix;
  private static FakeDFS dfs;
  
  private FakeDFS(String ip, int port){
    this.prefix = String.format("/tmp/%s:%d", ip, port);
    File file = new File(prefix);
    file.mkdirs();
  }
  
  public static DFS getConnection(String ip, int port){
    if(dfs == null){
      dfs = new FakeDFS(ip, port);
    }
    return dfs;
  }
  public DFSReader getReader(String dfsPath) throws Exception{
    String filePath = String.format("%s/%s", prefix, dfsPath);
    return new FakeDFSReader(filePath);
  }
  
  public DFSWriter getWriter(String dfsPath) throws Exception{
    String filePath = String.format("%s/%s", prefix, dfsPath);
    return new FakeDFSWriter(filePath);
  }
  
  public List<String> ls(String dfsDirPath){
    String folderPath = String.format("%s/%s", prefix, dfsDirPath);
    File folder = new File(folderPath);
    File[] listOfFiles = folder.listFiles(); 
    List<String> outputList = new ArrayList<String>();
    for(File f : listOfFiles){
      outputList.add(f.getName());
    }
    return outputList;
  }
  
  public Host getHost(String dfsPath, int version){
    int port = 9990 + version;
    return new Host("localhost", port);
  }
}
