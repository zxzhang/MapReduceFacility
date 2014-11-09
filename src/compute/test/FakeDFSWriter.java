package compute.test;

import java.io.FileNotFoundException;
import java.io.PrintStream;


public class FakeDFSWriter extends DFSWriter{
  PrintStream ps;
  public FakeDFSWriter(String filename) throws FileNotFoundException{
    ps = new PrintStream(filename);
  }
  public void println(String line){
    ps.println(line);
  }
}
