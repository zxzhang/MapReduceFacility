classpath=bin/
#mapper=compute.mapper.InvertedIndexMapper
#reducer=compute.reducer.InvertedIndexReducer
mapper=compute.mapper.WordCountMapper
reducer=compute.reducer.WordCountReducer
rmiregistryPort_1=10000
rmiregistryPort_2=10001
rmiregistryPort_3=10002
jobTracker_host=localhost
jobTracker_port=10000
localInputFile=data/apple_data.txt
localOutputDir=data/output
dfsInputDir=input
dfsOutputDir=output


compile:
	javac -d bin/ src/compute/*/*.java src/compute/*/*/*.java
clean:
	rm -rf bin/*
	rm -rf data/output/
	rm *.log
kill:
	killall java
	killall rmiregistry
rmi:
	cd bin/; rmiregistry $(rmiregistryPort_1) &
	cd bin/; rmiregistry $(rmiregistryPort_2) &
	cd bin/; rmiregistry $(rmiregistryPort_3) &
run:
	java -cp $(classpath) compute.job.JobTrackerServer $(jobTracker_port) > job_server.log & 
	@sleep 1
	java -cp $(classpath) compute.task.TaskTrackerServer $(rmiregistryPort_1) $(jobTracker_host) $(jobTracker_port)  > task_server_1.log &
	@sleep 1
	java -cp $(classpath) compute.task.TaskTrackerServer $(rmiregistryPort_2) $(jobTracker_host) $(jobTracker_port) > task_server_2.log &
	@sleep 1
	java -cp $(classpath) compute.task.TaskTrackerServer $(rmiregistryPort_3) $(jobTracker_host) $(jobTracker_port) > task_server_3.log &
	@sleep 1
	java -cp $(classpath) compute.client.JobClient $(jobTracker_host) $(jobTracker_port) $(localInputFile) $(dfsInputDir) $(localOutputDir) $(dfsOutputDir) $(mapper) $(reducer)
