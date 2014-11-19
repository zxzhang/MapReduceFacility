package compute.mapper;

import java.util.StringTokenizer;

/*
 * WordCountMapper.java
 * 
 * Author: San-Chuan Hung
 * 
 * Sample Mapper for word count.
 * 
 * */

public class WordCountMapper implements Mapper<String, Long> {
  public void map(String key, String value, Context context){
    String line = value;
    StringTokenizer tokenizer = new StringTokenizer(line);
    String word; 
    while (tokenizer.hasMoreTokens()) {
      word = tokenizer.nextToken();
      context.write(word, new Long(1));
    } 
  }
}
