package compute.task;

import java.util.List;

import compute.job.Job;

public class ReduceTask extends Task{
  List<String> localInputPaths;
  String dfsOutputPath; 
  Class reduceClass;
  int reducerNum;
  public int getReducerNum(){return reducerNum;}

  public ReduceTask(ReduceTask task){
    super();
    this.setTaskType(TaskType.REDUCE);
    this.setJob(task.getJob());
    
    this.dfsOutputPath = task.getDfsOutputPath();
    this.reduceClass = task.getReduceClass();
    this.localInputPaths = task.getLocalInputPaths();  
  }
  
  public ReduceTask(int reducerNum, 
                    String dfsOutputPath, 
                    Class reduceClass, 
                    List<String> localInputPaths,
                    Job job){
    super();
    this.setTaskType(TaskType.REDUCE);
    this.setJob(job);
    
    this.dfsOutputPath = String.format("%s/%d.out", dfsOutputPath, reducerNum);
    this.reduceClass = reduceClass;
    this.localInputPaths = localInputPaths;
  }
  
  public String getDfsOutputPath(){
    return this.dfsOutputPath;
  }
  public Class getReduceClass(){
    return reduceClass;
  }
  public List<String> getLocalInputPaths(){
    return localInputPaths;
  }
  
}
