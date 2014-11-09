package compute.reducer;

import java.util.Iterator;

public class WordCountReducer implements Reducer<String,Long,String,Long>{
  public void reduce(String key, Iterator<Long> values, OutputCollector<String, Long> output){
    Long total = (long) 0;
    while(values.hasNext()){
      total += values.next();
    }
    output.collect(key, total);
  }
}
