package compute.job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

import compute.dfs.DFS;
import compute.dfs.MasterDFS;
import compute.dfs.iostream.DFSReader;
import compute.dfs.iostream.DFSWriter;
import compute.dfs.util.ReadWriteLock;
// import jdk.internal.org.objectweb.asm.util.CheckFieldAdapter;
import compute.job.message.HeartbeatMessage;
import compute.job.message.JobTrackMessage;
import compute.mapper.Mapper;
import compute.reducer.Reducer;
import compute.scheduler.TaskScheduler;
import compute.task.MapTask;
import compute.task.ReducePreprocessTask;
import compute.task.ReduceTask;
import compute.task.TaskTracker;
import compute.utility.Host;

public class JobTrackerServer implements JobTracker {
  JobTable jobTable;

  TaskTrackerTable taskTrackerTable;

  DFS dfs;

  TaskScheduler taskScheduler;

  // ReadWriteLock readWriteLock = null;

  public String submitJob(String dfsInputPath, String dfsOutputPath,
          Class<? extends Mapper> mapper, Class<? extends Reducer> reducer) {
    // find all spilt input files
    List<String> splitInputFiles = dfs.ls(dfsInputPath);// must know the data's location

    // add into task queue
    Job job = jobTable.addJob(dfsInputPath, dfsOutputPath, mapper, reducer, splitInputFiles);

    // add job into task scheduler
    taskScheduler.addJob(job);

    return job.getJobId();
  }

  public JobTrackMessage trackJob(String jobId) {
    Job job = jobTable.get(jobId);
    return new JobTrackMessage(job);
  }

  public boolean deleteJob(String jobId) {
    return jobTable.removeJob(jobId);
  }

  public JobTrackerServer() {
    jobTable = new JobTable();
    taskTrackerTable = new TaskTrackerTable();
    dfs = new MasterDFS(taskTrackerTable);// FakeDFS.getConnection("localhost", 8888);
    taskScheduler = new TaskScheduler(dfs, taskTrackerTable);
  }

  public boolean register(TaskTracker taskTracker) throws RemoteException {
    try {
      taskTrackerTable.put(taskTracker.getTaskTrackerId(), taskTracker);
      taskTracker.ack();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public void run() throws InterruptedException {
    while (true) { // the loop executes each 1 secs
      // check
      if (taskScheduler.getPenndingMapTasksSize() > 0) {
        taskScheduler.schedulePendingMapTask();
      }
      if (taskScheduler.getFinishedMapTaskSize() > 0) {
        taskScheduler.scheduleFinishedMapTask();
      }
      if (taskScheduler.getPenndingReducePreprocessTasksSize() > 0) {
        taskScheduler.schedulePendingReducePreprocessTask();
      }
      if (taskScheduler.getFinishedReducePreprocessTasksSize() > 0) {
        taskScheduler.scheduleFinishedReducePreprocessTask();
      }
      if (taskScheduler.getPenndingReduceTasksSize() > 0) {
        taskScheduler.schedulePendingReduceTask();
      }
      if (taskScheduler.getFinishedReduceTaskSize() > 0) {
        List<Job> finishedJobs = taskScheduler.checkFinishedJobs();
        for (Job job : finishedJobs) {
          if (!jobTable.updateJobStatus(job.getJobId(), JobStatus.COMPLETED)) {
            System.out.println("Cannot update job status: " + job.getJobId());
          }
        }
      }

      System.out.println(taskScheduler);
      Thread.sleep(1000);
    }
  }

  public static void main(String[] args) {
    String host = "localhost";
    // launch JobTrackerServer
    try {
      System.out.println("Server init.");
      JobTrackerServer obj = new JobTrackerServer();
      JobTracker stub = (JobTracker) UnicastRemoteObject.exportObject(obj, 0);
      // Bind the remote object's stub in the RMI registry
      Registry registry = LocateRegistry.getRegistry(1099);

      registry.rebind("jobtracker", stub);
      System.out.println("Server ready");

      // obj.run();

      // /////////////////////////

      obj.startTinyShell();

      // /////////////////////////

    } catch (Exception e) {
      System.err.println("Server exception[RemoteException] : " + e.toString());
      e.printStackTrace();
    }
  }

  @Override
  public boolean heartbeat(String taskTrackerId, HeartbeatMessage hbm) throws RemoteException {
    System.out.println("Heart Beating.: " + taskTrackerId);

    // update TaskTracker updated time.
    taskTrackerTable.updateTime(taskTrackerId);
    taskTrackerTable.setTaskTrackerStats(taskTrackerId, hbm.getTaskTrackerStats());

    return true;
  }

  public boolean finishMapTask(MapTask task) {
    this.taskScheduler.finishMapTask(task);
    this.jobTable.updateMapTask(task.getJob().getJobId(), task);
    return true;
  }

  public boolean finishReducePreprocessTask(ReducePreprocessTask task) {
    // update taskScheduler
    this.taskScheduler.finishReducePreprocessTask(task);
    // update jobTable
    this.jobTable.updateReducePreprocessTask(task.getJob().getJobId(), task);

    return true;
  }

  public boolean finishReduceTask(ReduceTask task) {
    // update taskScheduler
    this.taskScheduler.finishReduceTask(task);
    // update jobTable
    this.jobTable.updateReduceTask(task.getJob().getJobId(), task);

    return true;
  }

  @Override
  public DFSReader getReader(String dfsPath) {
    if (dfsPath == null || dfsPath.length() == 0) {
      return null;
    }

    try {
      return dfs.getReader(dfsPath);
    } catch (Exception e) {
      System.out.println("Master getReader Error...");
      System.out.println(e.getMessage());
    }
    return null;
  }

  @Override
  public DFSWriter getWriter(String dfsPath) {
    if (dfsPath == null || dfsPath.length() == 0) {
      return null;
    }

    try {
      return dfs.getWriter(dfsPath);
    } catch (Exception e) {
      System.out.println("Master getWriter Error...");
      System.out.println(e.getMessage());
    }
    return null;
  }

  @Override
  public List<String> getLs(String dfsDirPath) {
    return dfs.ls(dfsDirPath);
  }

  @Override
  public Host getHost(String dfsPath, int version) {
    return dfs.getHost(dfsPath, version);
  }

  @Override
  public void addFile(String dfsPath, String localPath) {
    this.dfs.addFile(dfsPath, localPath);
  }

  @Override
  public void readLock(String dfsPath) {
    this.dfs.readLock(dfsPath);
  }

  @Override
  public void readUnLock(String dfsPath) {
    this.dfs.readUnLock(dfsPath);
  }

  @Override
  public void writeLock(String dfsPath) {
    this.dfs.writeLock(dfsPath);
  }

  @Override
  public void writeUnLock(String dfsPath) {
    this.dfs.writeUnLock(dfsPath);
  }

  public void startTinyShell() throws Exception {
    System.out.println("15640 project-1 ...");

    // doHelp();

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    while (true) {
      System.out.print("tsh> ");
      String command = null;

      try {
        command = br.readLine();
      } catch (IOException e) {
        System.out.println(e.getMessage());
        System.exit(1);
      }

      processCommand(command);
    }
  }

  private void processCommand(String command) throws Exception {
    if (command == null) {
      return;
    }

    String[] args = command.split("\\s+");
    if (args.length == 0) {
      return;
    }

    switch (args[0]) {
      case "slave":
        System.out.println(this.taskTrackerTable);
        break;
      case "test":
        this.dfs.addFile("/test/input", "/tmp/test.txt");
        break;
      default:
        System.out.println("Unknown command! Please input again...");
        break;
    }
  }
}
