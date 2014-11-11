package compute.task;

import java.io.Serializable;

import compute.job.Job;
import compute.utility.Host;


enum TaskType{
  MAP, REDUCE
}

public abstract class Task implements Serializable{
  static long maxTaskId = 0; 
  
  Long taskId; 
  Job job;
  TaskType taskType;
  TaskStatus taskStatus;
  Host host;
  
  public abstract void updateJob();
  public Job getJob(){return this.job;}
  public void setJob(Job job){this.job = job;}
  public TaskType getTaskType(){return taskType;}
  public void setTaskType(TaskType taskType){this.taskType = taskType;}
  public void setTaskStatus(TaskStatus taskStatus){this.taskStatus = taskStatus;}
  public TaskStatus getTaskStatus(){return taskStatus;}
  public void setHost(Host host){this.host = host;}
  public Host getHost(){return host;}
  
  public Task(){
    this.taskId = Task.maxTaskId;
    Task.maxTaskId += 1;
  }
  
  public String toString(){
    return String.format("'%d'%s:%s", taskId, taskType.toString(), taskStatus.toString());
  }
  
  public Long getTaskId(){
    return taskId;
  }
  
  public boolean equals(Object obj){
    Task task2 = (Task) obj;
    return this.getTaskId().equals(task2.getTaskId());
  }
}
