package compute.task;

import compute.job.Job;
import compute.utility.Host;

public class ReducePreprocessTask extends Task{
  int reducerNum;
  Host dataSourceHost;
  String localIntermediateFilePath; // assign by original map task local output
  String localSortedOutputFilePath;
  
  public int getReducerNum(){return reducerNum;}
  public Host getDataSourceHost(){return dataSourceHost;}
  public String getLocalIntermediateFilePath(){return localIntermediateFilePath;}
  public String getLocalSortedOutputFilePath(){return localSortedOutputFilePath;}
  public void setLocalSortedOutputFilePath(String localSortedOutputFilePath){
    this.localSortedOutputFilePath = localSortedOutputFilePath; 
  }
  
  public ReducePreprocessTask(int reducerNum, MapTask task){
    super();
    
    this.reducerNum = reducerNum;
    this.dataSourceHost = task.getHost();
    this.localIntermediateFilePath = task.getLocalOutputPath();
    this.localSortedOutputFilePath = String.format("%s_sorted", task.getLocalOutputPath());
  
    this.setJob(job);
    this.setTaskType(TaskType.REDUCEPREPROCESS);
    this.setTaskStatus(TaskStatus.PENDING);
  }
  
  


  
}
