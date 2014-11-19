package compute.reducer;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class InvertedIndexReducer implements Reducer<String, String, String, String>{

  @Override
  public void reduce(String key, Iterator<String> values, OutputCollector<String, String> output) {
    Set<String> documents = new HashSet<String>();
    StringBuilder sb = new StringBuilder(); 
    while(values.hasNext()){
      String document = values.next(); 
      if(!documents.contains(document)){
        sb.append(values.next());
        sb.append(" ");
        documents.add(document);
      }
    }
    output.collect(key, sb.toString());
    
  }

}
