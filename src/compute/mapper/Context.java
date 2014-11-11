package compute.mapper;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;

import compute.configure.AllConfiguration;

public class Context {
  String localDir; 
  String prefix;
  
  ObjectOutputStream[] oosArray;
  
  public Context(String inputPrefix){
    this.localDir = localDir;
    this.prefix = prefix;
    oosArray = new ObjectOutputStream[AllConfiguration.numOfReducer];
    for(int i = 0; i < oosArray.length; i++){
      // i = the corresponding reducer id 
      String filename = String.format("%s_%d", inputPrefix, i);
      try {
        oosArray[i] = new ObjectOutputStream(new FileOutputStream(filename));
      } catch (Exception e) {
        System.out.println("Cannot open file."+ filename);
        e.printStackTrace();
      }
    }
  }
  
  public boolean write(Object keyout, Object valueout){
    int reducerId = keyout.hashCode() % AllConfiguration.numOfReducer;
    try{
      oosArray[reducerId].writeObject(keyout);
      oosArray[reducerId].writeObject(valueout);
    }catch(Exception e){
      System.out.println(
          String.format(
              "Cannot write key/value (%s/%s)", 
              keyout.toString(), valueout.toString()
          )
      );
      e.printStackTrace();
      return false;
    }
    return true;
  }
  
  public void close(){
    for(ObjectOutputStream oos : oosArray){
      try {
        oos.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        System.out.println("Cannot close file.");
        e.printStackTrace();
      }
    }
  }
}
