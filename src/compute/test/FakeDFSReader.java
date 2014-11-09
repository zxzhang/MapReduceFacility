package compute.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FakeDFSReader extends DFSReader{
  BufferedReader bf;
  
  public FakeDFSReader(String filename) throws FileNotFoundException{
    bf = new BufferedReader(new FileReader(filename));
  }
  @Override
  public String readLine() throws IOException{
    return bf.readLine();
  }

}