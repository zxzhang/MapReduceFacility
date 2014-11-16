package compute.task.box;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import compute.dfs.DFS;
import compute.dfs.iostream.DFSWriter;
import compute.reducer.OutputCollector;
import compute.reducer.Reducer;
import compute.task.ReduceTask;
import compute.task.Task;

public class ReduceTaskBox extends TaskBox {
  DFS dfs;

  public ReduceTaskBox(ReduceTask task,  DFS dfs, Callback callback) {
    super(task, callback);
    this.dfs = dfs;
  }

  

  Map<Object, List<Object>> getData(List<String> list){
    
    Map<Object, List<Object>> ret = new HashMap<Object, List<Object>>();
    for(String file : list){
      ObjectInputStream ois;
      try {
        ois = new ObjectInputStream(new FileInputStream(new File(file)));
      } catch(Exception e){
        System.out.println("Cannot open file: " +file );
        e.printStackTrace();
        return null;
      }
      
      while(true){
        try{
          Object key = ois.readObject();
          Object value = ois.readObject();
          if(!ret.containsKey(key)){
            ret.put(key, new ArrayList<Object>());
          }
          ret.get(key).add(value);
        }catch(Exception e){
          break;//EOF
        }
      }
    }
    return ret;
  }
  
  void innerRun() {
    // create outputCollector
    ReduceTask reduceTask = (ReduceTask) task;
    

    OutputCollector outputCollector = null;
    
    try {
       outputCollector = new OutputCollector(
                        dfs.getWriter(reduceTask.getDfsOutputPath()));
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    
    // apply reduce function and write
    // merge sort partial files
    Reducer reducer = null;
    try {
      reducer = (Reducer) reduceTask.getReduceClass().newInstance();
    } catch (Exception e){
      System.out.println("Cannot cast reducer.");
      return;
    }
    
    Map<Object, List<Object>> data = getData(reduceTask.getLocalInputPaths()); 
    for(Object key: data.keySet()){
      Iterator values = data.get(key).iterator();
      reducer.reduce(key, values, outputCollector);
    }
  }

}
