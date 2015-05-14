
clearCaches () { sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"; }

saveHadoopLogs () { ls -t /usr/local/hadoop/logs/userlogs/| head -n $1 | tail -n 1 | xargs -t -I{} sh -c "cat /usr/local/hadoop/logs/userlogs/{}/*/syslog" | grep -E 'TaskImpl|TaskAttemptImpl'  > $2 ; }

LOG_FILENAME=run.log
NUMBER_RUNS=1
INPUT_FILE=
DATE=$(date +"%s")
IOSTAT_INTERVAL=5
TOP_INTERVAL=$IOSTAT_INTERVAL
jobsToRun=()
OPTIND=1 

while getopts "r:j:i:n:" opt; do
  case $opt in
    r)
      echo "Results folder= $OPTARG" >&2
			RESULTS_FOLDER=$OPTARG
      ;;
    j)
      echo "New job= $OPTARG" >&2
			jobsToRun+=("$OPTARG")
      ;;
    i)
      echo "input file= $OPTARG" >&2
			INPUT_FILE=$OPTARG
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
START_TIME=${RESULTS_FOLDER}\starttime_
TASK_DURATION=${RESULTS_FOLDER}\tasks_duration_job
CPU_USAGE=${RESULTS_FOLDER}\cpu
DISK_USAGE=${RESULTS_FOLDER}\disk
jobTasksDurationLog=()


set -x

jobPids=()
for i in $(seq "$NUMBER_RUNS")
do
	echo $i
	#Create tasks duration files for each job
	for j in $(seq "${#jobsToRun[@]}")
	do
		echo "allo"
		jobTasksDurationLog[$j]=${TASK_DURATION}$i\-$j.log
		echo ${jobTasksDurationLog[$j]}
	done

	#Create CPU and DISK files
	CURR_START_TIME=${START_TIME}\-$i.csv
	CURR_DISK_USAGE=${DISK_USAGE}\-$i.csv
	CURR_CPU_USAGE=${CPU_USAGE}\-$i.csv
	echo $CURR_DISK_USAGE
	echo $CURR_CPU_USAGE

	for j in "${jobsToRun[@]}"
	do
		echo $j
		clearCaches
		sh -c "$j" &
		jobPids+=($!)
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
		#saveHadoopLogs $j ${jobTasksDurationLog[$j]}
		echo $j
	done
done
