import java.io.BufferedReader;
import java.io.FileReader;

public class FakeDFSReader extends DFSReader{
  BufferedReader bf;
  
  public FakeDFSReader(String filename){
    bf = new BufferedReader(new FileReader(filename));
  }
  public String readLine(){
    return bf.readLine();
  }
}