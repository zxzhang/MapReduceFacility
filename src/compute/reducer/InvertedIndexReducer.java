package compute.reducer;

import java.util.Iterator;

public class InvertedIndexReducer implements Reducer<String, String, String, String>{

  @Override
  public void reduce(String key, Iterator<String> values, OutputCollector<String, String> output) {
    
    StringBuilder sb = new StringBuilder(); 
    while(values.hasNext()){
      sb.append(values.next());
      sb.append(" ");
    }
    output.collect(key, sb.toString());
    
  }

}
