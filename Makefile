classpath=bin/
#mapper=compute.mapper.InvertedIndexMapper
#reducer=compute.reducer.InvertedIndexReducer
mapper=compute.mapper.WordCountMapper
reducer=compute.reducer.WordCountReducer
rmiregistryPort=1099
localInputFile=data/apple_data.txt
localOutputDir=data/output
dfsInputDir=input
dfsOutputDir=output


deploy:
	cd data; python deploy.py
kill:
	killall java
rmi:
	cd bin/; rmiregistry &
run:
	java -cp $(classpath) compute.job.JobTrackerServer > job_server.log & 
	@sleep 1
	java -cp $(classpath) compute.task.TaskTrackerServer localhost $(rmiregistryPort) > task_server.log &
	@sleep 1
#   java -cp $(classpath) compute.job.JobTrackerServer > job_server.log
	java -cp $(classpath) compute.client.JobClient localhost $(localInputFile) $(dfsInputDir) $(localOutputDir) $(dfsOutputDir) $(mapper) $(reducer)
