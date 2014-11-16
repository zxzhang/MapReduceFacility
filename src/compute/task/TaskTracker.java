package compute.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.rmi.Remote;
import java.rmi.RemoteException;

import compute.job.JobTracker;

public interface TaskTracker extends Remote {
  public String getTaskTrackerId() throws RemoteException;

  public void ack() throws RemoteException;

  public String getHostName() throws RemoteException;

  public int getPort() throws RemoteException;

  public boolean assignMapTask(MapTask task) throws RemoteException;

  public boolean assignReducePreprocessTask(ReducePreprocessTask task) throws RemoteException;

  public boolean assignReduceTask(ReduceTask task) throws RemoteException;

  public byte[] getByte(String filename, long pos, int length) throws RemoteException;

  public long getBufferReader(String filename) throws RemoteException;

  public String readLine(long br) throws RemoteException;

  public long getPrintStream(String filename) throws RemoteException;

  public void printLine(long ps, String line) throws RemoteException;
  
  public void removeRead(long read);
  
  public void removeWrite(long write);
}
