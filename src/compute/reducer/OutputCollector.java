package compute.reducer;

import compute.dfs.iostream.DFSWriter;

public class OutputCollector<KEY, VALUE>{
  DFSWriter dfsWriter;
  public OutputCollector(DFSWriter dfsWriter){
    this.dfsWriter = dfsWriter;
  }
  
  void collect(KEY key, VALUE value){
    this.dfsWriter.println(String.format("%s\t%s", key.toString(), value.toString()));
  }
}
