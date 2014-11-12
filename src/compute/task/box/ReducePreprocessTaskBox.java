package compute.task.box;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import compute.myio.RemoteIOUtility;
import compute.task.ReducePreprocessTask;
import compute.task.Task;
import compute.utility.Host;
import compute.utility.LocalIOUtility;

class ObjectPair{
  Object first;
  Object second;
  public ObjectPair(Object first, Object second){
    this.first = first;
    this.second = second;
  }
  public Object getFirst(){
    return first;
  }
  public Object getSecond(){
    return second;
  }
}

public class ReducePreprocessTaskBox extends TaskBox{

  public ReducePreprocessTaskBox(ReducePreprocessTask task, Callback callback) {
    super(task, callback);
  }

  @Override
  void innerRun() {
    // fetch data from remote source host 
    ReducePreprocessTask preprocessTask = (ReducePreprocessTask) this.getTask();
    Host souceFileHost = preprocessTask.getDataSourceHost();
    
    String intermediateFile = preprocessTask.getLocalSortedOutputFilePath() + "_notfinished";
    
    RemoteIOUtility.copyFile(
        souceFileHost, 
        preprocessTask.getLocalIntermediateFilePath(), 
        intermediateFile
    );
    
    ObjectInputStream ois = null;
    try {
      ois = new ObjectInputStream(new FileInputStream(new File(intermediateFile)));
    } catch (Exception e){
      System.out.println("Cannot open localfile: "+ intermediateFile);
      return ;
    }
    // sort the data 
    ArrayList<ObjectPair> objectList = new ArrayList<ObjectPair>();
    
    while(true){
      try{
        Object key = ois.readObject();
        if(key == null){
          ois.close();
          break;
        }
        Object value = ois.readObject();
        objectList.add(new ObjectPair(key, value));
      }catch(EOFException e1){
        break;
      }catch(Exception e){
        e.printStackTrace();
        return;
      }
    }
    
    Collections.sort(objectList, new Comparator<ObjectPair>(){
      @Override
      public int compare(ObjectPair objPair1, ObjectPair  objPair2)
      {
        Object key1 = objPair1.getFirst();
        Object key2 = objPair2.getFirst();
        return key1.hashCode() - key2.hashCode();
      }
    });
    
    // write into the local file system
    ObjectOutputStream oos = null;
    try {
      oos = new ObjectOutputStream(
                new FileOutputStream(
                    new File(
                        preprocessTask.getLocalSortedOutputFilePath()
                    )
                )
            );
    } catch (Exception e){
      System.out.println("Cannot open ObjectOutputStream. ");
      return;
    }
    
    for(ObjectPair pair : objectList){
      Object key = pair.getFirst();
      Object value = pair.getSecond();
      try {
        oos.writeObject(key);
        oos.writeObject(value);
      } catch (IOException e) {
        System.out.println("Cannot write object: "+ key + "\t" + value);
        e.printStackTrace();
      }
    }
    
    try {
      oos.close();
    } catch (IOException e) {
      System.out.println("Cannot close Object Output Stream.");
      return;
    }
    // delete intermediate file
    File f = new File(intermediateFile);
    f.delete();
  }
}
