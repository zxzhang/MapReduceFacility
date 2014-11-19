package compute.configure;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ReadConfFile {
  public static void readConfFile() throws IOException {
    
    BufferedReader br = new BufferedReader(new FileReader("conf/Configuration.conf"));
    
    try {
      
      String line = br.readLine();
      while (line != null) {
        String[] str = line.trim().split("\\:");
        
//        if (line.startsWith("Number of Mappers:")) {
//          AllConfiguration.numOfMapper = Integer.parseInt(str[str.length - 1].trim());
//        } else if (line.startsWith("Number of Reducers:")) {
        if(line.startsWith("Number of Reducers:")){
          AllConfiguration.numOfReducer = Integer.parseInt(str[str.length - 1].trim());
        } else if (line.startsWith("Number of Slaves:")) {
          AllConfiguration.numOfSlaves = Integer.parseInt(str[str.length - 1].trim());
        } else if (line.startsWith("Block File length:")) {
          AllConfiguration.blockFileLength = Integer.parseInt(str[str.length - 1].trim());
        } else if (line.startsWith("Replicate:")) {
          AllConfiguration.replicate = Integer.parseInt(str[str.length - 1].trim());
        } else if (line.startsWith("Max Number of Mapper:")) {
          TaskTrackerConfiguration.maxNumOfMapper = Integer.parseInt(str[str.length - 1].trim());
        } else if (line.startsWith("Max Nunber of Reducer:")) {
          TaskTrackerConfiguration.maxNumOfReducer = Integer.parseInt(str[str.length - 1].trim());
        } else if (line.startsWith("Max Number of Reduce Preprocess:")) {
          TaskTrackerConfiguration.maxNumOfReducePreprocess = Integer.parseInt(str[str.length - 1].trim());
        }
        
        line = br.readLine();
      }

    } finally {
      br.close();
    }
  }

  public static void printConfFile() {
    System.out.println("All Configuration: ");
//    System.out.println("Number of Mappers: " + AllConfiguration.numOfMapper);
    System.out.println("Number of Reducers: " + AllConfiguration.numOfReducer);
    System.out.println("Number of Slaves: " + AllConfiguration.numOfSlaves);
    System.out.println("Block File length: " + AllConfiguration.blockFileLength);
    System.out.println("Replicate: " + AllConfiguration.replicate);
    System.out.println();
    System.out.println("TaskTrackerConfiguration:");
    System.out.println("Max Number of Mapper: " + TaskTrackerConfiguration.maxNumOfMapper);
    System.out.println("Max Nunber of Reducer: " + TaskTrackerConfiguration.maxNumOfReducer);
    System.out.println("Max Number of Reduce Preprocess: "
            + TaskTrackerConfiguration.maxNumOfReducePreprocess);
    System.out.println();
  }

  public static void main(String[] args) throws IOException {
    printConfFile();
    readConfFile();
    printConfFile();
  }
}
