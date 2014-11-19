package compute.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import compute.configure.ReadConfFile;
import compute.configure.TaskTrackerConfiguration;
import compute.dfs.DFS;
import compute.dfs.SlaveDFS;
import compute.dfs.iostream.DFSReader;
import compute.job.Job;
import compute.job.JobTracker;
import compute.job.message.HeartbeatMessage;
//import compute.myio.RMIInputStream;
//import compute.myio.RMIInputStreamImpl;
//import compute.myio.RMIInputStreamInterf;
import compute.task.box.Callback;
import compute.task.box.MapCallback;
import compute.task.box.MapTaskBox;
import compute.task.box.ReduceCallback;
import compute.task.box.ReducePreprocessCallback;
import compute.task.box.ReducePreprocessTaskBox;
import compute.task.box.ReduceTaskBox;
import compute.utility.Host;
import compute.utility.HostUtility;
import compute.utility.LocalIOUtility;

class MapTaskItem {
  MapTask task;
}

public class TaskTrackerServer implements TaskTracker {
  String taskTrackerId;

  String hostName;

  int port;

  JobTracker jobTracker;

  String localSpacePath;

  DFS dfs;

  HashMap<Long, BufferedReader> readerTable = null;

  HashMap<Long, PrintStream> printTable = null;

  AtomicLong ioId = null;

  Deque<MapTask> pendingMapTasks;

  Deque<ReducePreprocessTask> pendingReducePreprocessTasks;

  Deque<ReduceTask> pendingReduceTasks;

  Deque<MapTask> runningMapTasks;

  Deque<ReducePreprocessTask> runningReducePreprocessTasks;

  Deque<ReduceTask> runningReduceTasks;

  Deque<MapTask> finishedMapTasks;

  Deque<ReducePreprocessTask> finishedReducePreprocessTasks;

  Deque<ReduceTask> finishedReduceTasks;  
  
  
  public void setJobTracker(JobTracker jobTracker){ this.jobTracker = jobTracker;}
  public void setDFS(DFS dfs){ this.dfs = dfs;}
  public String getHostName(){ return this.hostName;}
  public int getPort(){ return port; }
  public String getTaskTrackerId(){ return this.taskTrackerId;}
  public void setTaskTrackerId(String taskTrackerId){ this.taskTrackerId = taskTrackerId;} 
  
  public TaskTrackerServer(String hostName, int port){

    pendingMapTasks = new LinkedList<MapTask>();
    pendingReducePreprocessTasks = new LinkedList<ReducePreprocessTask>();
    pendingReduceTasks = new LinkedList<ReduceTask>();

    runningMapTasks = new LinkedList<MapTask>();
    runningReducePreprocessTasks = new LinkedList<ReducePreprocessTask>();
    runningReduceTasks = new LinkedList<ReduceTask>();

    finishedMapTasks = new LinkedList<MapTask>();
    finishedReducePreprocessTasks = new LinkedList<ReducePreprocessTask>();
    finishedReduceTasks = new LinkedList<ReduceTask>();

    this.hostName = hostName;
    this.port = port;

    this.localSpacePath = LocalIOUtility.getLocalSpace(this);

    this.readerTable = new HashMap<Long, BufferedReader>();
    this.printTable = new HashMap<Long, PrintStream>();
    this.ioId = new AtomicLong(0);
  }

  public void ack() {
    System.out.println("Register OK.");
  }

  /****************************************************************************/

  public int getMapTaskSlot() {
    return TaskTrackerConfiguration.maxNumOfMapper
            - (this.pendingMapTasks.size() + this.runningMapTasks.size());
  }

  public int getReducePreprocessTaskSlot() {
    return TaskTrackerConfiguration.maxNumOfReducePreprocess
            - (this.pendingReducePreprocessTasks.size() + this.runningReducePreprocessTasks.size());
  }

  public int getReduceTaskSlot() {
    return TaskTrackerConfiguration.maxNumOfReducer
            - (this.pendingReduceTasks.size() + this.runningReduceTasks.size());
  }

  /****************************************************************************/

  public void addPendingMapTask(MapTask task) {
    String[] tmp = task.getDfsInputPath().split("/");
    String subFilename = tmp[tmp.length - 1];

    task.setLocalOutputPath(String.format("%s/%s", localSpacePath, subFilename));
    this.pendingMapTasks.add(task);
  }

  public void removePendingMapTask(MapTask task) {
    this.pendingMapTasks.remove(task);
  }

  public void addRunningMapTask(MapTask task) {
    this.runningMapTasks.add(task);
  }

  public void removeRunningMapTask(MapTask task) {
    this.runningMapTasks.remove(task);
  }

  public void addFinishedMapTask(MapTask task) {
    this.finishedMapTasks.add(task);
  }

  public void removeFinishedMapTask(MapTask task) {
    this.finishedMapTasks.remove(task);
  }

  public void addPendingReducePreprocessTask(ReducePreprocessTask task) {
    String[] tmp = task.getLocalIntermediateFilePath().split("/");
    String subFilename = tmp[tmp.length - 1];

    task.setLocalSortedOutputFilePath(String.format("%s/sorted_%s", localSpacePath, subFilename));

    this.pendingReducePreprocessTasks.add(task);
  }

  public void removePendingReducePreprocessTask(ReducePreprocessTask task) {
    this.pendingReducePreprocessTasks.remove(task);
  }

  public void addRunningReducePreprocessTask(ReducePreprocessTask task) {
    this.runningReducePreprocessTasks.add(task);
  }

  public void removeRunningReducePreprocessTask(ReducePreprocessTask task) {
    this.runningReducePreprocessTasks.remove(task);
  }

  public void addFinishedReducePreprocessTask(ReducePreprocessTask task) {
    this.finishedReducePreprocessTasks.add(task);
  }

  public void removeFinishedReducePreprocessTask(ReducePreprocessTask task) {
    this.finishedReducePreprocessTasks.remove(task);
  }

  public void addPendingReduceTask(ReduceTask task) {
    this.pendingReduceTasks.add(task);
  }

  public void removePendingReduceTask(ReduceTask task) {
    this.pendingReduceTasks.remove(task);
  }

  public void addRunningReduceTask(ReduceTask task) {
    this.runningReduceTasks.add(task);
  }

  public void removeRunningReduceTask(ReduceTask task) {
    this.runningReduceTasks.remove(task);
  }

  public void addFinishedReduceTask(ReduceTask task) {
    this.finishedReduceTasks.add(task);
  }

  public void removeFinishedReduceTask(ReduceTask task) {
    this.finishedReduceTasks.remove(task);
  }

  /****************************************************************************/

  public boolean assignMapTask(MapTask task) {
    if (getMapTaskSlot() > 0) {
      System.out.println("Assign Task" + task.toString());

      // update host
      task.setHost(new Host(hostName, port));
      addPendingMapTask(task);
      return true;
    }
    return false;
  }

  public boolean assignReducePreprocessTask(ReducePreprocessTask task) {

    if (getReducePreprocessTaskSlot() > 0
            && getMapTaskSlot() == TaskTrackerConfiguration.maxNumOfMapper) { // if no map tasks
                                                                              // running and have
                                                                              // slot
      System.out.println("Assign Task" + task.toString());

      addPendingReducePreprocessTask(task);
      return true;
    }
    return false;
  }

  public boolean assignReduceTask(ReduceTask task) { // if no map tasks running and have slot
    if (getReduceTaskSlot() > 0 && getMapTaskSlot() == TaskTrackerConfiguration.maxNumOfMapper) {
      System.out.println("Assign Task" + task.toString());

      addPendingReduceTask(task);
      return true;
    }
    return false;
  }

  /****************************************************************************/

  public void checkPendingMapTask() {
    MapCallback callback = new MapCallback(this);

    Iterator<MapTask> mapTasksIter = pendingMapTasks.iterator();
    while (mapTasksIter.hasNext()) {
      MapTask task = mapTasksIter.next();
      // create MapTaskBox
      MapTaskBox taskBox = new MapTaskBox(task, dfs, callback);
      // run taskbox
      taskBox.start();
      // move to running MapTask
      mapTasksIter.remove();
      addRunningMapTask(task);
    }
  }

  public void checkFinishedMapTask() {
    Iterator<MapTask> mapTasksIter = finishedMapTasks.iterator();

    while (mapTasksIter.hasNext()) {
      MapTask task = mapTasksIter.next();
      try {
        if (jobTracker.finishMapTask(task)) {
          mapTasksIter.remove();
        }
      } catch (RemoteException e) {
        e.printStackTrace();
        continue;
      }
    }
  }

  public void checkPendingReducePreprocessTask() {
    ReducePreprocessCallback callback = new ReducePreprocessCallback(this);

    Iterator<ReducePreprocessTask> reducePreprocessTaskIter = pendingReducePreprocessTasks
            .iterator();
    while (reducePreprocessTaskIter.hasNext()) {
      ReducePreprocessTask task = reducePreprocessTaskIter.next();
      // create ReduceTaskBox
      ReducePreprocessTaskBox taskBox = new ReducePreprocessTaskBox(task, callback);
      // run taskbox
      taskBox.start();
      // move to running MapTask
      reducePreprocessTaskIter.remove();
      addRunningReducePreprocessTask(task);
    }
  }

  public synchronized void checkFinishedReducePreprocessTask() {
    Iterator<ReducePreprocessTask> reducePreprocessTasksIter = finishedReducePreprocessTasks
            .iterator();

    while (reducePreprocessTasksIter.hasNext()) {
      ReducePreprocessTask task = reducePreprocessTasksIter.next();
      try {
        if (jobTracker.finishReducePreprocessTask(task)) {
          reducePreprocessTasksIter.remove();
        }
      } catch (RemoteException e) {
        e.printStackTrace();
        continue;
      }
    }
  }

  public synchronized void checkPendingReduceTask() {
    ReduceCallback callback = new ReduceCallback(this);

    Iterator<ReduceTask> reduceTaskIter = pendingReduceTasks.iterator();
    while (reduceTaskIter.hasNext()) {
      ReduceTask task = reduceTaskIter.next();
      // create ReduceTaskBox
      ReduceTaskBox taskBox = new ReduceTaskBox(task, dfs, callback);
      // run taskbox
      taskBox.start();
      // move to running MapTask
      reduceTaskIter.remove();
      addRunningReduceTask(task);
    }
  }

  public synchronized void checkFinishedReduceTask() {
    Iterator<ReduceTask> reduceTasksIter = finishedReduceTasks.iterator();

    while (reduceTasksIter.hasNext()) {
      ReduceTask task = reduceTasksIter.next();
      try {
        if (jobTracker.finishReduceTask(task)) {
          reduceTasksIter.remove();
        }
      } catch (RemoteException e) {
        e.printStackTrace();
        continue;
      }
    }
  }

  /****************************************************************************/

  public void run() throws InterruptedException, RemoteException {

    while (true) { // the loop executes each 1 secs
      // check
      checkPendingMapTask();
      checkFinishedMapTask();
      checkPendingReducePreprocessTask();
      checkFinishedReducePreprocessTask();
      checkPendingReduceTask();
      checkFinishedReduceTask();
      // report heartbeat
      int mapTaskSlot = getMapTaskSlot();
      int reducePreprocessTaskSlot = getReducePreprocessTaskSlot();
      int reduceTaskSlot = getReduceTaskSlot();
      this.jobTracker.heartbeat(this.getTaskTrackerId(), new HeartbeatMessage(mapTaskSlot,
              reducePreprocessTaskSlot, reduceTaskSlot));

      Thread.sleep(1000);

    }
  }

  /****************************************************************************/

  public byte[] getByte(String filename, long pos, int length) {
    byte[] b = new byte[length];
    RandomAccessFile r = null;
    try {
      r = new RandomAccessFile(new File(filename), "r");
    } catch (FileNotFoundException e1) {
      System.out.println("Cannot find file: " + filename);
      e1.printStackTrace();
      return null;
    }
    // read file
    int c = -1;
    try {
      r.seek(pos);
      c = r.read(b);
      r.close();
    } catch (IOException e) {
      System.out.println("Cannot read file: " + filename);
      e.printStackTrace();
      return null;
    }

    if (c < 0) {
      // if no bytes: return null;
      return null;
    } else {
      if (c < length) {
        b = Arrays.copyOf(b, c);
      }
      return b;
    }
  }

  /****************************************************************************/

  public static void main(String[] args) throws Exception {
    ReadConfFile.readConfFile();
    
    int taskTrackerPort = Integer.parseInt(args[0]);
    String jobTrackerHost = args[1];
    int jobTrackerPort = Integer.parseInt(args[2]);

    // launch local server for task tracker
    String localHostName = HostUtility.getHostName();

    TaskTrackerServer taskTracker = new TaskTrackerServer(localHostName, taskTrackerPort);
    TaskTracker stub = (TaskTracker)UnicastRemoteObject.exportObject(taskTracker, 0);
    Registry registry = LocateRegistry.getRegistry(taskTrackerPort);

    registry.rebind("tasktracker", stub);

    // build connection with JobTracker
    try {
        Registry remoteRegistry = LocateRegistry.getRegistry(jobTrackerHost, jobTrackerPort);
        JobTracker jobTracker = (JobTracker) remoteRegistry.lookup("jobtracker");
        String taskTrackerId = jobTracker.register(stub);
        if(taskTrackerId == null){
          System.err.println("Cannot register JobTracker: " + jobTrackerHost);
          System.exit(0);
        } else{
          stub.setTaskTrackerId(taskTrackerId);
        }
        taskTracker.setJobTracker(jobTracker);

        // build connection with DFS
        DFS dfs = new SlaveDFS(jobTracker);
        taskTracker.setDFS(dfs);
    } catch (Exception e) {
      System.err.println("Client exception: " + e.toString());
      e.printStackTrace();
    }

    // run taskTracker
    taskTracker.run();
  }

  @Override
  public long getBufferReader(String filename) {
    long id = this.ioId.incrementAndGet();

    try {
      this.readerTable.put(id, new BufferedReader(new FileReader(filename)));
      return id;
    } catch (FileNotFoundException e) {
      System.out.println(e.getMessage());
      return -1;
    }
  }

  @Override
  public String readLine(long br) {
    try {
      return this.readerTable.get(br).readLine();
    } catch (IOException e) {
      System.out.println(e.getMessage());
      return null;
    }
  }

  @Override
  public long getPrintStream(String filename) {
    String[] tmp = filename.split("/");
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < tmp.length - 1; i++) {
      sb.append("/").append(tmp[i]);
    }

    String dir = sb.toString();
    File dirf = new File(dir);
    dirf.mkdirs();

    long id = this.ioId.incrementAndGet();
    
    try {
      this.printTable.put(id, new PrintStream(filename));
      return id;
    } catch (FileNotFoundException e) {
      System.out.println(e.getMessage());
      return -1;
    }
  }

  @Override
  public void printLine(long ps, String line) {
    this.printTable.get(ps).println(line);
  }

  @Override
  public void removeRead(long read) {
    try {
      this.readerTable.get(read).close();
    } catch (IOException e) {
      System.out.println("Read close IO error...");
      System.out.println(e.getMessage());
    }
    this.readerTable.remove(read);
  }

  @Override
  public void removeWrite(long write) {
    this.printTable.get(write).close();
    this.printTable.remove(write);
  }

}
