package compute.utility;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class HostUtility {
  public static String getHostName() throws UnknownHostException{
    return InetAddress.getLocalHost().getHostName();
  }
}
