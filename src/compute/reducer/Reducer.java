package compute.reducer;

import java.util.Iterator;

import compute.mapper.Context;

/*
 * Reducer.java
 * 
 * Author: San-Chuan Hung
 * 
 * Reducer interface
 * 
 * */


public interface Reducer<K2,V2,K3,V3>{
  void reduce(K2 key, Iterator<V2> values, OutputCollector<K3,V3> output);
}
