package compute.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Arrays;


public class FakeDFSWriter extends DFSWriter{
  PrintStream ps;
  public FakeDFSWriter(String filename) throws FileNotFoundException{
    String[] tmp = filename.split("/");
    StringBuilder sb = new StringBuilder();
    for(int i =0 ;i < tmp.length -1 ;i++){
      sb.append("/");
      sb.append(tmp[i]);
    }
    String dir = sb.toString();
    File dirf = new File(dir);
    dirf.mkdirs();
    
    ps = new PrintStream(filename);
  }
  public void println(String line){
    ps.println(line);
  }
}
