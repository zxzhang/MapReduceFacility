package compute.dfs.util;

import java.io.Serializable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/*
 * ReadWriteLock.java
 * 
 * Author: Zhengxiong Zhang
 * 
 * The Read Lock and the Write Lock of DFS files
 * 
 * */

public class ReadWriteLock implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = -5443711410231214875L;

  public ReadWriteLock() {
    this.readWriteLock = new ReentrantReadWriteLock();
    this.ReadLock = readWriteLock.readLock();
    this.WriteLock = readWriteLock.writeLock();
  }

  private ReentrantReadWriteLock readWriteLock = null;

  private WriteLock WriteLock = null;

  private ReadLock ReadLock = null;

  public void readLock() {
    ReadLock.lock();
  }

  public void readUnlock() {
    ReadLock.unlock();
  }

  public void writeLock() {
    WriteLock.lock();
  }

  public void writeUnlock() {
    WriteLock.unlock();
  }
}
