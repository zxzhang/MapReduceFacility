package compute.utility;

import java.net.InetAddress;
import java.net.UnknownHostException;

/*
 * HostUtility.java
 * 
 * Author: San-Chuan Hung
 * 
 * The utility helps a server to find local host name.
 * 
 * */

public class HostUtility {
  public static String getHostName() throws UnknownHostException{
    return InetAddress.getLocalHost().getHostName();
  }
}
