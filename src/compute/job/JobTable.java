package compute.job;


import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

class IDGenerator{
  private SecureRandom random = new SecureRandom();
  
  public String nextStringID(){
    return new BigInteger(130, random).toString(8);
  }
}

public class JobTable {
  Map<String, Job> tableMap;
  IDGenerator idGenerator;
  
  
  public JobTable(){
    tableMap = new HashMap<String, Job>();
    idGenerator = new IDGenerator();
  }
  
  public String addJob(String dfsInputPath, Class< ? extends Mapper> mapper, Class<? extends Reducer> reducer){
    // get job id
    String jobId;
    while(true){
      jobId = idGenerator.nextStringID();
      if(tableMap.containsKey(jobId)){
        jobId = idGenerator.nextStringID();
      }else{
        break;
      }
    }
    
    // insert into tableMap
    tableMap.put(jobId, new Job(jobId, dfsInputPath, mapper, reducer)); 
    
    return jobId;
  }
  
  public Job get(String jobId){
     return tableMap.get(jobId);
  }
}
