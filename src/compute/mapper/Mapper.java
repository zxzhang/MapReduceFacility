package compute.mapper;

/*
 * Mapper.java
 * 
 * Author: San-Chuan Hung
 * 
 * Mapper's interface
 * 
 * */

public interface Mapper<KEYOUT,VALUEOUT> {  
  void map(String key, String value, Context context) ;
}
