package compute.task.box;

import compute.task.ReducePreprocessTask;
import compute.task.Task;

public class ReducePreprocessTaskBox extends TaskBox{

  public ReducePreprocessTaskBox(ReducePreprocessTask task, Callback callback) {
    super(task, callback);
  }

  @Override
  void innerRun() {
    // fetch data from remote source host 
    
    
    // sort the data 
    
    // write into the local file system
    
  }
}
