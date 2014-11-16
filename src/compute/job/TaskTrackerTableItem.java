package compute.job;

import java.io.Serializable;

import compute.task.TaskTracker;
import compute.utility.Host;

public class TaskTrackerTableItem implements Serializable {

  TaskTracker taskTracker;

  long lastUpdateTime; // secs

  Host host;

  TaskTrackerStats taskTrackerStats;

  public TaskTrackerTableItem(TaskTracker taskTracker, Host host) {
    this.taskTracker = taskTracker;
    this.host = host;
    this.updateTime();
  }

  public Host getHost() {
    return host;
  }

  public long updateTime() {
    this.lastUpdateTime = System.currentTimeMillis() / 1000;
    return lastUpdateTime;
  }

  public TaskTracker getTaskTracker() {
    return taskTracker;
  }

  public TaskTrackerStats getStats() {
    return taskTrackerStats;
  }

  public void setTaskTrackerStats(TaskTrackerStats taskTrackerStats) {
    this.taskTrackerStats = taskTrackerStats;
  }
}