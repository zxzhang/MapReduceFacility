package compute.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import compute.job.JobStatus;
import compute.job.JobTracker;
import compute.job.message.JobTrackMessage;


public class JobClient {
  JobTracker jobTracker;
  
  public JobClient(JobTracker jobTracker){
    this.jobTracker = jobTracker;
  }
  
  public void trackJob(String jobId) throws RemoteException, InterruptedException{
    while(true){
      Thread.sleep(3000);

      JobTrackMessage jtm = jobTracker.trackJob(jobId);
      System.out.println(jtm);
      
      if(jtm.getJobStatus() == JobStatus.COMPLETED){
        System.out.println("The job is complepted, and the result is on the DFS: "+ jtm.getDfsOutputPath());
        jobTracker.deleteJob(jobId);
        break;
      }else if(jtm.getJobStatus() == JobStatus.PENDING){
        continue;
      }else if(jtm.getJobStatus() == JobStatus.RUNNING){
        continue;
      }
    }
  }
  
  public void run(String[] args) throws ClassNotFoundException, RemoteException, InterruptedException{
    
    String dfsInputPath = args[1];
    String dfsOutputPath = args[2];
    String mapperClassStr = args[3];
    String reducerClassStr = args[4];
    Class mapper = Class.forName(mapperClassStr);
    Class reducer = Class.forName(reducerClassStr);
    
    String jobId = jobTracker.submitJob(dfsInputPath, dfsOutputPath, mapper, reducer);
    if(jobId==null){
      System.out.println("Something went wrong.");
    }else{
      this.trackJob(jobId);
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
  
  public static void main(String[] args) throws NotBoundException, IOException, ClassNotFoundException, InterruptedException{
    // host 
    String jobTrackerHost = args[0];
    
    Registry remoteRegistry = LocateRegistry.getRegistry(jobTrackerHost);
    JobTracker jobTracker = (JobTracker) remoteRegistry.lookup("jobtracker");
    
    JobClient jobClient = new JobClient(jobTracker);
    
    jobClient.run(args);
  }
}
