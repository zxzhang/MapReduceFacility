package compute.utility;

import java.io.Serializable;

/*
 * Host.java
 * 
 * Author: San-Chuan Hung
 * 
 * The class to identify a host (url + port).
 * 
 * */

public class Host implements Serializable {
  String url;

  int port;

  public Host(String url, int port) {
    this.url = url;
    this.port = port;
  }

  public String getUrl() {
    return url;
  }

  public int getPort() {
    return port;
  }

  public boolean equals(Object obj) {
    Host host2 = (Host) obj;
    if (this.url.equals(host2.getUrl()) && this.port == host2.port) {
      return true;
    }
    return false;
  }
  
  public int hashCode(){
    return this.url.hashCode() + port ;
  }

  public String toString() {
    return String.format("[host] %s:%d", url, port);
  }
}
