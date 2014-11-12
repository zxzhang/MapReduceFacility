package compute.myio;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import com.healthmarketscience.rmiio.RemoteInputStreamClient;

import compute.task.TaskTracker;
import compute.utility.Host;

public class RemoteIOUtility {
  public static boolean copyFile(Host remoteHost, String remotePath, String localPath){
    Registry remoteRegistry;
    TaskTracker taskTracker = null;

    try {
      remoteRegistry = LocateRegistry.getRegistry(remoteHost.getUrl(), remoteHost.getPort());
      taskTracker = (TaskTracker) remoteRegistry.lookup("tasktracker");
    } catch (Exception e) {
      System.out.println("Cannot find remote host : "+remoteHost);
      e.printStackTrace();
      return false;
    }
    
    long pos = (long) 0;
    int length = 4096;
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(new File(localPath));
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    while(true){
      byte[] b=null;
      try {
        b = taskTracker.getByte(remotePath, pos, length);
      } catch (RemoteException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      
      if(b != null){
        pos += b.length;
        try {
          fos.write(b);
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }else{
        break;
      }
    }
        
    try {
      fos.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    return true;
  }
  
  public static void main(String[] args){
    // test code
    
    RemoteIOUtility.copyFile(
        new Host("San-Chuans-MacBook-Air.local", 1099), 
        "/tmp/localhost:8888/input/apple_data.txt_aa", 
        "/tmp/apple_data.txt_aa"
    );
    
  }
}
