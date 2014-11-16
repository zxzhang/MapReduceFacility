package compute.job;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import compute.dfs.iostream.DFSReader;
import compute.dfs.iostream.DFSWriter;
import compute.job.message.HeartbeatMessage;
import compute.job.message.JobTrackMessage;
import compute.mapper.Mapper;
import compute.reducer.Reducer;
import compute.task.MapTask;
import compute.task.ReducePreprocessTask;
import compute.task.ReduceTask;
import compute.task.TaskTracker;
import compute.utility.Host;

public interface JobTracker extends Remote {

  public String submitJob(String dfsInputPath, String dfsOutputPath,
          Class<? extends Mapper> mapper, Class<? extends Reducer> reducer) throws RemoteException;

  public JobTrackMessage trackJob(String jobId) throws RemoteException;

  public boolean register(TaskTracker taskTracker) throws RemoteException;

  public boolean heartbeat(String taskTrackerId, HeartbeatMessage hbm) throws RemoteException;

  public boolean finishMapTask(MapTask task) throws RemoteException;

  public boolean finishReducePreprocessTask(ReducePreprocessTask task) throws RemoteException;

  public boolean finishReduceTask(ReduceTask task) throws RemoteException;

  public DFSReader getReader(String dfsPath) throws RemoteException;;

  public DFSWriter getWriter(String dfsPath) throws RemoteException;;

  public List<String> getLs(String dfsDirPath) throws RemoteException;;

  public Host getHost(String dfsPath, int version) throws RemoteException;;

  public void addFile(String dfsPath, String localPath) throws RemoteException;;

  public boolean deleteJob(String jobId) throws RemoteException;

  public void readLock(String dfsPath) throws RemoteException;;

  public void readUnLock(String dfsPath) throws RemoteException;;

  public void writeLock(String dfsPath) throws RemoteException;;

  public void writeUnLock(String dfsPath) throws RemoteException;;
}
