package compute.mapper;

import java.util.StringTokenizer;

public class WordCountMapper implements Mapper<String, String, String, Long> {
  public void map(String key, String value, Context context){
    String line = value;
    StringTokenizer tokenizer = new StringTokenizer(line);
    String word; 
    while (tokenizer.hasMoreTokens()) {
      word = tokenizer.nextToken();
      context.write(word, 1);
    } 
  }
}
