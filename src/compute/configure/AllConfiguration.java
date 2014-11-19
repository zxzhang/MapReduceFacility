package compute.configure;

/*
 * AllConfiguration.java
 * 
 * Author: San-Chuan Hung
 * 
 * These parameters are for MapReduce and DFS internal settings
 * 
 * */

public class AllConfiguration {
  public static int numOfMapper = 4;
  public static int numOfReducer = 2;
  public static int blockFileLength = 500000;
  public static int replicate = 3;
  public static int numOfSlaves = 3;
  public static int taskTrackerDieOutTime = 2; // secs
}
