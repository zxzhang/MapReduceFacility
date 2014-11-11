package compute.task;

import java.io.Serializable;

import compute.job.Job;


enum TaskType{
  MAP, REDUCE
}

public abstract class Task implements Serializable{
  Job job;
  TaskType taskType;
  TaskStatus taskStatus;
  public Job getJob(){return this.job;}
  public void setJob(Job job){this.job = job;}
  public TaskType getTaskType(){return taskType;}
  public void setTaskType(TaskType taskType){this.taskType = taskType;}
  public void setTaskStatus(TaskStatus taskStatus){this.taskStatus = taskStatus;}
}
