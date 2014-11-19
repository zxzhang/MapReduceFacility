package compute.reducer;

import java.util.Iterator;

/*
 * WordCountReducer.java
 * 
 * Author: San-Chuan Hung
 * 
 * Sample reducer for word count.
 * 
 * */


public class WordCountReducer implements Reducer<String,Long,String,Long>{
  public void reduce(String key, Iterator<Long> values, OutputCollector<String, Long> output){
    Long total = (long) 0;
    while(values.hasNext()){
      total += values.next();
    }
    output.collect(key, total);
  }
}
