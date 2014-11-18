package compute.reducer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;

import javax.imageio.stream.FileImageInputStream;

import compute.utility.Pair;

public class ReduceInputGenerator {
  String[] inputPaths;
  ObjectInputStream[] oiss;
  Object[] currentKeys ;
  Object[] currentValues;
  
  public ReduceInputGenerator(String[] inputPaths){
    this.inputPaths = inputPaths;
    this.oiss = new ObjectInputStream[inputPaths.length];
    for(int i = 0; i < inputPaths.length; i++){
      try {
        oiss[i] = new ObjectInputStream(
                    new FileInputStream(
                        new File(this.inputPaths[i])
                    )
                 );
      } catch(Exception e ){
        System.out.println("Error: Cannot open file "+this.inputPaths[i]);
        continue;      
      }
    }
    
    currentKeys = new Object[inputPaths.length];
    currentValues = new Object[inputPaths.length];
  }

}
