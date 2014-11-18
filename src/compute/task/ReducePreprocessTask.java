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
  
  public ReducePreprocessTask(ReducePreprocessTask oldTask){
    super();
    
    this.reducerNum = oldTask.getReducerNum();
    this.dataSourceHost = oldTask.getHost();
    this.localIntermediateFilePath = oldTask.getLocalIntermediateFilePath();
    this.localSortedOutputFilePath = oldTask.getLocalSortedOutputFilePath();
    
    this.setJob(oldTask.job);
    this.setTaskType(TaskType.REDUCEPREPROCESS);
    this.setTaskStatus(oldTask.getTaskStatus());
  }
  
  public ReducePreprocessTask(int reducerNum, MapTask task){
    super();

    this.reducerNum = reducerNum;
    this.dataSourceHost = task.getHost();
    // the intermediate file will be "outputfile_{reducerNum}"
    this.localIntermediateFilePath = String.format("%s_%d", task.getLocalOutputPath(), reducerNum);
    this.localSortedOutputFilePath = String.format("%s_%d_sorted", task.getLocalOutputPath(), reducerNum);
  
    this.setJob(task.job);
    this.setTaskType(TaskType.REDUCEPREPROCESS);
    this.setTaskStatus(TaskStatus.PENDING);
  }
  
  public void setJob(Job job){    
    this.job = job;
    job.addReducePreprocessTask(this);
  }


  
}
