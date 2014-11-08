import java.io.*;



public class FakeDFS extends DFS {
  String prefix;

  public static DFS getConnection(String ip, int port){
    prefix = String.format("/tmp/%s_%d", ip, port);
    File file = new File(prefix);
    file.mkdirs();
  }
  public DFSReader getReader(String dfsPath) throws Exception{
    String filePath = String.format("%s/%s", prefix, dfsPath);
    return new FakeDFSReader(filePath);
  }
  
  public DFSWriter getWriter(String dfsPath) throws Exception{
    String filePath = String.format("%s/%s", prefirx, dfsPath);
    return new FakeDFSWriter(filePath);
  }
  
  public List<String> ls(String dfsDirPath){
    String folderPath = String.format("%s/%s", prefix, dfsDirPath);
    File folder = new File(folderPath);
    File[] listOfFiles = folder.listFiles(); 
    List<String> outputList = new ArrayList<String>();
    for(File f : listOfFiles){
      outputList.append(f.getName());
    }
    return outputList;
  }
  
}
