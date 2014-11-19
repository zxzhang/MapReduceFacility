package compute.utility;

import java.io.File;

import compute.task.TaskTrackerServer;

/*
 * LocalIOUtility.java
 * 
 * Author: San-Chuan Hung
 * 
 * The utility mapper to store intermediate files in it's local file system
 * 
 * */

public class LocalIOUtility {
  static String spacePrefix = "tmp/mapreduce";
  public static String getLocalSpace(TaskTrackerServer taskTrackerServer){
    String host = taskTrackerServer.getHostName();
    int port = taskTrackerServer.getPort();
    String dirPath = String.format("%s/%s:%d/", spacePrefix, host, port);
    File dir = new File(dirPath); 
    if(dir.exists()){
      dir.delete();
    }    
    dir.mkdirs();
    return dirPath;
  }
}
