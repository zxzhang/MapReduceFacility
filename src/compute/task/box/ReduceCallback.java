package compute.task.box;

import compute.task.MapTask;
import compute.task.ReduceTask;
import compute.task.TaskTrackerServer;

/*
 * ReduceCallback.java
 * 
 * Author: San-Chuan Hung
 * 
 * The implementation of call back in ReduceTaskBox.
 * 
 * */

public class ReduceCallback implements Callback{
  TaskTrackerServer taskTracker ;
  public ReduceCallback(TaskTrackerServer taskTracker){
    this.taskTracker = taskTracker;
  }
  @Override
  public void callBack(TaskBox box) {
    // TODO Auto-generated method stub
    ReduceTask task = (ReduceTask) box.getTask();
    
    // move the task from running to finished
    taskTracker.removeRunningReduceTask(task);
    taskTracker.addFinishedReduceTask(task);
  }

}
