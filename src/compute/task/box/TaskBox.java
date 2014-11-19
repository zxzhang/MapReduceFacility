package compute.task.box;

import compute.task.Task;

/*
 * TaskBox.java
 * 
 * Author: San-Chuan Hung
 * 
 * The abstract class of box, which lets task run individually.
 * 
 * */

public abstract class TaskBox extends Thread{
  Task task;
  Callback callback;
  public TaskBox(Task task, Callback callback){
    this.task = task;
    this.callback = callback;
  }
  
  public Task getTask(){
    return task;
  }
  
  abstract void innerRun();
  public void run(){
    innerRun();
    callback.callBack(this);
  }
}
