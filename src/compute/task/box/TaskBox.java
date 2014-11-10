package compute.task.box;

import compute.task.Task;

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
