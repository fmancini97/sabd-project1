#!/bin/bash


cleanup() {
	if [[ "${HDFS_MODE}" == "master" ]]; then 
		$HADOOP_HDFS_HOME/sbin/stop-dfs.sh;
	fi
}

#Trap SIGTERM
trap 'cleanup' SIGTERM;



if [[ -n "${HDFS_WORKERS}" ]]; then
	IFS=',' read -ra WORKERS <<< "${HDFS_WORKERS}"
	for worker in "${WORKERS[@]}"; do
		echo $worker >> $HADOOP_HOME/etc/hadoop/workers;
	done
fi


if [[ "${HDFS_MODE}" == "worker" ]]; then
	echo "Starting worker node";
	service ssh start;
	#$HADOOP_HDFS_HOME/sbin/start-dfs.sh;
elif [[ "${HDFS_MODE}" == "master" ]]; then
	echo "Starting master node";
	service ssh start;
	hdfs namenode -format;
	$HADOOP_HDFS_HOME/sbin/start-dfs.sh;
	echo "Master node is up and running"
else 
	echo "HDFS_MODE is not correctly set";
	exit 1;
fi

while true; do sleep 1000; done

#Wait
wait $!;
