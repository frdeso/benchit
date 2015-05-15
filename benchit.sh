
clearCaches () { sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"; }

saveHadoopLogs () { 
TASKID=$1
STARTTIME=$2
OUTPUT=$3
TEMPFILE="$(mktemp)";
ls -t /usr/local/hadoop/logs/userlogs/| head -n $TASKID | tail -n 1 | xargs -t -I{} sh -c "cat /usr/local/hadoop/logs/userlogs/{}/*/syslog" | grep -E 'TaskImpl|TaskAttemptImpl'  > $TEMPFILE ; 
python3 taskDuration.py $TEMPFILE $STARTTIME > $OUTPUT;
}

LOG_FILENAME=run.log
NUMBER_RUNS=1
INPUT_FILE=
DATE=$(date +"%s")
IOSTAT_INTERVAL=5
TOP_INTERVAL=$IOSTAT_INTERVAL
jobsToRun=()
OPTIND=1 

while getopts "r:j:n:" opt; do
  case $opt in
    r)
      echo "Results folder= $OPTARG" >&2
			RESULTS_FOLDER=$OPTARG
      ;;
    j)
      echo "New job= $OPTARG" >&2
			jobsToRun+=("$OPTARG")
      ;;
    n)
      echo "Number of runs= $OPTARG" >&2
			NUMBER_RUNS=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done

RESULTS_FOLDER+=${DATE}/
echo $INPUT_FILE
echo $RESULTS_FOLDER
echo $NUMBER_RUNS
echo ${jobsToRun[*]}

#Save all inputs to a file
mkdir -p $RESULTS_FOLDER
LOG_FILE=$RESULTS_FOLDER$LOG_FILENAME
echo "Input file: $INPUT_FILE" >> $LOG_FILE
echo "Number of runs: $NUMBER_RUNS" >> $LOG_FILE

for j in "${jobsToRun[@]}"
do
	echo "$j" >> $LOG_FILE
done

# Job execution
START_TIME=${RESULTS_FOLDER}\starttime
TASK_DURATION=${RESULTS_FOLDER}\tasks_duration_job
CPU_USAGE=${RESULTS_FOLDER}\cpu
DISK_USAGE=${RESULTS_FOLDER}\disk
SUMMARY=${RESULTS_FOLDER}/summary
TEMP_FOLDER=/tmp/tempfolder/

set -x

jobPids=()
for i in $(seq "$NUMBER_RUNS")
do
	echo $i
	jobTasksDurationLog=()
	jobTempFolder=()
	#Create tasks duration files for each job and temp result files
	for j in $(seq "${#jobsToRun[@]}")
	do
		echo $j
		jobTasksDurationLog[$j]=${TASK_DURATION}$i\-$j.log
		jobTempFolder[$j]=${TEMP_FOLDER}$j
		echo ${jobTempFolder[$j]}
	done

	#Create CPU and DISK files
	CURR_START_TIME=${START_TIME}\-$i.csv
	CURR_DISK_USAGE=${DISK_USAGE}\-$i.csv
	CURR_CPU_USAGE=${CPU_USAGE}\-$i.csv
	CURR_SUMMARY=${SUMMARY}\-$i.csv
	echo $CURR_DISK_USAGE
	echo $CURR_CPU_USAGE
	
	clearCaches

	jobId=1
	echo "Launching jobs"
	for j in "${jobsToRun[@]}"
	do
		echo $j
		CMD=$j
		CMD+=" "
		CMD+=${jobTempFolder[$jobId]}
		sh -c "$CMD" &
		jobPids+=($!)
		echo "${jobTempFolder[$jobId]}"
		((jobId++))
		echo $jobId
	done

	echo "cpu" > $CURR_CPU_USAGE
	echo "read,write,utilization" > $CURR_DISK_USAGE
	date +%s > $CURR_START_TIME

	#Launch monitoring tools
	monitoringPids=()
	top -b -d $TOP_INTERVAL | grep "Cpu(s)" | awk '{printf "%f\n", $2 + $4}' >> $CURR_CPU_USAGE &
	monitoringPids+=($!)
	iostat -y -x $IOSTAT_INTERVAL | grep sda | awk '{printf "%f, %f, %f\n", $6, $7, $14}' >> $CURR_DISK_USAGE  &
	monitoringPids+=($!)

	wait ${jobPids[@]}
	kill $(pidof top)
	kill $(pidof iostat)

	wait ${monitoringPids[@]}

	clearCaches

	for j in $(seq "${#jobsToRun[@]}")
	do
		saveHadoopLogs $j $CURR_START_TIME ${jobTasksDurationLog[$j]}
		echo $j
		hdfs dfs -rm -r -skipTrash ${jobTempFolder[$j]}
	done
	paste -d',' $CURR_CPU_USAGE $CURR_DISK_USAGE > $CURR_SUMMARY
done
