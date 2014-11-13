package compute.utility;

public class Pair<V1, V2> {
  V1 first;
  V2 second;
  public Pair(V1 first, V2 second){
    this.first = first;
    this.second = second;
  }
  public V1 getFirst(){return first;}
  public V2 getSecond(){return second;}
  public boolean equals(Object obj){
    Pair<V1, V2> pair = (Pair<V1, V2>) obj;

    boolean isEqual = pair.getFirst().equals(first) && pair.getSecond().equals(second);
    
    if(isEqual){
      return true;
    }
    return false;
  }
  public int hashCode(){
    

    return this.first.hashCode() + this.second.hashCode();
  }
}
