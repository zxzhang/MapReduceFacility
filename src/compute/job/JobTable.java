package compute.job;


import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import compute.mapper.Mapper;
import compute.reducer.Reducer;

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
  
  public Job addJob(String dfsInputPath, String dfsOutputPath, Class< ? extends Mapper> mapper, Class<? extends Reducer> reducer, List<String> splitInputFiles){
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
    
    Job job = new Job(jobId, dfsInputPath, mapper, reducer, splitInputFiles);
    // insert into tableMap
    tableMap.put(jobId, job); 
    
    return job;
  }
  
  public Job get(String jobId){
     return tableMap.get(jobId);
  }
}
