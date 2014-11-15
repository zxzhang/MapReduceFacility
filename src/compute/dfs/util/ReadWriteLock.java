package compute.dfs.util;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class ReadWriteLock {

  private static class SingletonHolder {
    public static ReadWriteLock _instance = new ReadWriteLock();
  }

  public synchronized static ReadWriteLock getInstance() {
    return SingletonHolder._instance;
  }

  private ReadWriteLock() {
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
