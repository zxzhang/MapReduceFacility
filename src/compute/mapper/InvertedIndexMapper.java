package compute.mapper;

public class InvertedIndexMapper implements Mapper<String, String>{
  public void map(String key, String value, Context context){
    String[] tmp = value.trim().split(" ");
    for(String word : tmp){
      context.write(word, key);
    }
  }
}
