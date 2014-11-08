import java.io.PrintStream;


public class FakeDFSWriter extends DFSWriter{
  PrintStream ps;
  public FakeDFSWriter(String filename){
    ps = new PrintStream(filename);
  }
  public void println(Stirng line){
    ps.println(line);
  }
}
