package compute.configure;

public class AllConfiguration {
  public static int numOfMapper = 4;
  public static int numOfReducer = 6;
  public static final int blockFileLength = 500000;
  public static final int replicate = 3;
  public static final int numOfSlaves = 3;
  public static int taskTrackerDieOutTime = 2; // secs
}
