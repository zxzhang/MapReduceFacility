package compute.mapper;

/*
 * InvertedIndexMapper.java
 * 
 * Author: San-Chuan Hung
 * 
 * Sample Mapper for inverted index.
 * 
 * */

public class InvertedIndexMapper implements Mapper<String, String>{
  public void map(String key, String value, Context context){
    String[] tmp = value.trim().split(" ");
    for(String word : tmp){
      context.write(word, key);
    }
  }
}
