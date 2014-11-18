package compute.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

import compute.configure.ReadConfFile;
import compute.dfs.iostream.DFSReader;
import compute.job.JobStatus;
import compute.job.JobTracker;
import compute.job.message.JobTrackMessage;
import compute.myio.RemoteIOUtility;


public class JobClient {
  JobTracker jobTracker;
  
  public JobClient(JobTracker jobTracker){
    this.jobTracker = jobTracker;
  }
  
  public boolean trackJob(String jobId) throws RemoteException, InterruptedException{
    boolean isCompleted = false;
    while(true){
      Thread.sleep(3000);

      JobTrackMessage jtm = jobTracker.trackJob(jobId);
      System.out.println(jtm);
      
      if(jtm.getJobStatus() == JobStatus.COMPLETED){        
        jobTracker.deleteJob(jobId);
        isCompleted =  true;
        break;
      }else if(jtm.getJobStatus() == JobStatus.PENDING){
        continue;
      }else if(jtm.getJobStatus() == JobStatus.RUNNING){
        continue;
      }
    }
    return isCompleted;
  }
  
  public void downloadFile(JobTracker jobTracker, String dfsFilePath, String localFilePath) throws Exception{
    PrintStream ps = new PrintStream(localFilePath);
    
    DFSReader dfsReader = jobTracker.getReader(dfsFilePath);
    String line = null;
    while((line = dfsReader.readLine()) != null){
      ps.println(line);
    }
    ps.close();
  }
  
  public void downloadFiles(JobTracker jobTracker, String dfsOutputDirPath, String localOutputDirPath) throws Exception{
    File f = new File(localOutputDirPath);
    if(!f.exists()){
      f.mkdirs();
    }
    
    List<String> dfsOutputFilePaths = jobTracker.getLs(dfsOutputDirPath);
    
    for(String dfsOutputFilePath : dfsOutputFilePaths){
      String dfsOutputFilename = RemoteIOUtility.getFilename(dfsOutputFilePath);
      String localFilePath = localOutputDirPath + "/" + dfsOutputFilename;
      downloadFile(jobTracker, dfsOutputFilePath, localFilePath);
    }
  }
  
  public void run(String[] args) throws Exception{
    
    String localInputPath = args[1];
    String dfsInputPath = args[2];
    String localOutputPath = args[3];
    String dfsOutputPath = args[4];
    String mapperClassStr = args[5];
    String reducerClassStr = args[6];
    Class mapper = Class.forName(mapperClassStr);
    Class reducer = Class.forName(reducerClassStr);
    
    // upload file onto DFS 
    dfsInputPath = "/" + dfsInputPath;
    dfsOutputPath = "/" + dfsOutputPath;
    
    String[] tmp = localInputPath.split("/");
    String filename = tmp[ tmp.length - 1];
    String dfsInputFilePath = dfsInputPath + "/" + filename;
    jobTracker.addFile(dfsInputFilePath, localInputPath);
    
    // submit Job
    String jobId = jobTracker.submitJob(dfsInputPath, dfsOutputPath, mapper, reducer);
    boolean isCompleted = false;
    if(jobId==null){
      System.out.println("Something went wrong.");
      return;
    }else{
      isCompleted = this.trackJob(jobId);
    }
    
    // download all files 
    if(isCompleted){
      downloadFiles(jobTracker, dfsOutputPath, localOutputPath);
      System.out.println(
          String.format(
              "Job finished succesfully. The output files are in %s on DFS and in %s on local",
              dfsOutputPath,
              localOutputPath
          )
      );
    }else{
      System.out.println("The job fails. Please try it again.");
    }
  }
  
//  public void shell() throws IOException, ClassNotFoundException, InterruptedException{
//    BufferedReader br = 
//        new BufferedReader(new InputStreamReader(System.in));
//    String line;
//    while(true){
//      System.out.print(">>>>: ");
//      line = br.readLine();
//      line = line.trim();
//      
//      String[] tmp = line.split(" ");
//      
//      if(tmp[0].equals("quit")){
//        System.out.println("Good bye ~ . :D");
//        System.exit(0);
//      }else if(tmp[0].equals("run")){
//        //run $(dfsInputPath) $(dfsOtuputPath) $(mapperClass) $(reducerClass)
//        if(tmp.length < 5 ){ 
//          System.out.println("--not enough args: run $(dfsInputPath) $(dfsOtuputPath) $(mapperClass) $(reducerClass)");
//          continue;
//        }
//        run(tmp);
//      }
//      
//    }
//  }
  
  public static void main(String[] args) throws Exception{
    ReadConfFile.readConfFile();
    
    // host 
    String jobTrackerHost = args[0];
    
    Registry remoteRegistry = LocateRegistry.getRegistry(jobTrackerHost);
    JobTracker jobTracker = (JobTracker) remoteRegistry.lookup("jobtracker");
    
    JobClient jobClient = new JobClient(jobTracker);
    
    jobClient.run(args);
  }
}
