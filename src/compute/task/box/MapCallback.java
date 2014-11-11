package compute.task.box;

import compute.task.MapTask;
import compute.task.TaskTrackerServer;

public class MapCallback implements Callback{
  TaskTrackerServer taskTracker ;
  public MapCallback(TaskTrackerServer taskTracker){
    this.taskTracker = taskTracker;
  }
  
  @Override
  public void callBack(TaskBox box) {
    // TODO Auto-generated method stub
    MapTask task = (MapTask) box.getTask();
    // move the task from running to finished
    taskTracker.removeRunningMapTask(task);
    taskTracker.addFinishedMapTask(task);
  }
}
