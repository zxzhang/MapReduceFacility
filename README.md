MapReduceFacility
=================


# 15-440 Project #3: Building a MapReduce Facility

Name: San-Chuan Hung, Zhengxiong Zhang
AndrewID: sanchuah, zhengxiz 

# System Requirement

    It needs JDK 1.7 or further version to compile the source code.
    
    It needs “rmiregistry” to start a java rmi registry server. 

# How to compile the code?
    
    type “make compile”

#How to start it? 

## Script

### Launch rmiregistry server

    type “make rmi”

    Configure input file, output file, map class, reduce class in Makefile
    
    We provide sample settings for two tasks: WordCount and InvertedIndex in Makefile setting.

### Run 
    
    type “make run”

    It launch a coordinator, two participants, and a job client connecting to coordinator for uploading the input files, submitting the job, tracking job progress, and retrieving the output from DFS. 

### Terminate all servers and rmiregistry

    type “make kill”

## By Hand

### Launch a coordinator

    type “rmiregistry $(coordinator_port)” 
    
    type “java -cp bin/ compute.job.JobTrackerServer $(coordinator_port) “

    e.g. java -cp bin/ compute.job.JobTrackerServer 10000

### Launch a participant

    type “rmiregistry $(participant_port)”

    type “java -cp $(classpath) compute.task.TaskTrackerServer $(participant_port) $(coordinator_host) $(coordinator_port)

    e.g. java -cp bin/ compute.task.TaskTrackerServer 10000 localhost 10000

### Launch a job client

    type “java -cp bin/ compute.client.JobClient $(coordinator_host) $(coordinator_port) $(local input file) $(input directory on DFS) $(local output dir) $(output directory on DFS) $(mapper class) $(reducer class)”

    e.g. java -cp bin/ compute.client.JobClient localhost 10000 data/apple_data.txt input data/output output compute.mapper.WordCountMapper compute.reducer.WordCountReducer


