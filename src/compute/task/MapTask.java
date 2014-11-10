package compute.task;

import compute.mapper.Mapper;

public class MapTask extends Task{
  String dfsInputPath;
  String localOutputPath; // assign by task tracker
  Class mapperClass;
  
  public MapTask(String dfsInputPath, Class mapperClass){
    this.setTaskType(TaskType.MAP);
    this.dfsInputPath = dfsInputPath;
//    this.localOutputPath = localOutputPath;
    this.mapperClass = mapperClass;
  }
  
  public String getDfsInputPath(){
    return dfsInputPath;
  }
  public String getLocalOutputPath(){
    return localOutputPath;
  }
  public Class getMapperClass(){
    return mapperClass;
  }
}
