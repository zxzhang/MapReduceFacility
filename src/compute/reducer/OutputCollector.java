package compute.reducer;

import compute.dfs.iostream.DFSWriter;

/*
 * OutputCollector.java
 * 
 * Author: San-Chuan Hung
 * 
 * This class collects all output key/value pairs from reducers.
 * 
 * */


public class OutputCollector<KEY, VALUE>{
  DFSWriter dfsWriter;
  public OutputCollector(DFSWriter dfsWriter){    
    this.dfsWriter = dfsWriter;
  }
  
  void collect(KEY key, VALUE value){
    try {
      this.dfsWriter.println(String.format("%s\t%s", key.toString(), value.toString()));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
