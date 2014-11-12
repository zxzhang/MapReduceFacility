package compute.task.box;

import compute.task.MapTask;
import compute.task.ReducePreprocessTask;
import compute.task.TaskTracker;
import compute.task.TaskTrackerServer;

public class ReducePreprocessCallback implements Callback{
  TaskTrackerServer taskTracker ;
  
  public ReducePreprocessCallback(TaskTrackerServer taskTracker){
    this.taskTracker = taskTracker;
  }
  
  @Override
  public void callBack(TaskBox box) {
    ReducePreprocessTask task = (ReducePreprocessTask) box.getTask();
    // move the task from running to finished
    taskTracker.removeRunningReducePreprocessTask(task);
    taskTracker.addFinishedReducePreprocessTask(task);
  }
}
