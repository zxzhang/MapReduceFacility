package compute.task;

import compute.job.Job;
import compute.mapper.Mapper;

public class MapTask extends Task{
  String dfsInputPath;
  String localOutputPath; // assign by task tracker
  Class mapperClass;
  
  public MapTask(String dfsInputPath, Class mapperClass){
    super();
    this.setTaskType(TaskType.MAP);
    this.dfsInputPath = dfsInputPath;
    this.mapperClass = mapperClass;
  }
  
  public void updateJob(){    
    if(!this.job.removeMapTask(this)){
      System.out.println("Cannot update MapTask["+this+"] in Job.");
    }
    this.job.addMapTask(this);
    System.out.println("afters");
    System.out.println(this.job.mapTasks);
    
  }
  
  public void setJob(Job job){    
    this.job = job;
    job.addMapTask(this);
  }
  
  public String getDfsInputPath(){
    return dfsInputPath;
  }
  public String getLocalOutputPath(){
    return localOutputPath;
  }
  
  public void setLocalOutputPath(String localOutputPath){
    this.localOutputPath = localOutputPath;
  }
  
  public Class getMapperClass(){
    return mapperClass;
  }
}
