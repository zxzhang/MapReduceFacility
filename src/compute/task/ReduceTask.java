package compute.task;

public class ReduceTask extends Task{
  String mapperHost;
  String mapperInputPath;
  String dfsOutputPath; 
  Class reduceClass;
  
  public ReduceTask(
      String mapperHost, 
      String mapperInputPath, 
      String dfsOutputPath, 
      Class reduceClass){
    super();
    this.mapperHost = mapperHost;
    this.mapperInputPath = mapperInputPath;
    this.dfsOutputPath = dfsOutputPath;
    this.reduceClass = reduceClass;
  }
  
  public String getMapperHost(){
    return this.mapperHost;
  }
  public String getMapperInputPath(){
    return this.mapperInputPath;
  }
  public String getDfsOutputPath(){
    return this.dfsOutputPath;
  }
  public Class getReduceClass(){
    return reduceClass;
  }

  @Override
  public void updateJob() {
    // TODO Auto-generated method stub
    
  }
  
}
