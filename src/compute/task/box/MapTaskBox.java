package compute.task.box;

import java.lang.reflect.Type;

import compute.mapper.Context;
import compute.mapper.Mapper;
import compute.task.MapTask;
import compute.test.DFS;
import compute.test.DFSReader;

public class MapTaskBox extends TaskBox{
  DFS dfs;
  public MapTaskBox(MapTask task, DFS dfs, MapCallback callback){
    super(task, callback); 
    this.dfs = dfs;
  }
  
  @Override
  void innerRun() {
    // TODO Auto-generated method stub
    MapTask task = (MapTask) this.getTask();
    String dfsInputPath = task.getDfsInputPath();
    
    // initial mapper instance
    Mapper mapper = null;
    try {
      mapper = (Mapper) task.getMapperClass().newInstance();
    } catch (Exception e1) {
      System.out.println("Cannot initial class :" + task.getMapperClass().toString());
      e1.printStackTrace();
    }
    
    Type mySuperclass = mapper.getClass().getGenericSuperclass();

    // create Context 
    Context context = new Context(task.getLocalOutputPath());
    
    // do map 
    try {
      DFSReader dfsReader = dfs.getReader(dfsInputPath);
      String line = null;
      while((line = dfsReader.readLine()) != null){
        String[] segs = line.trim().split("\t", 2);
        String key = segs[0];
        String value = segs[1];
        mapper.map(key, value, context);
      }
    } catch (Exception e) {
      // Cannot open file
      System.out.println("Cannot open file:" + dfsInputPath);
      e.printStackTrace();
    }
  }
  
}
