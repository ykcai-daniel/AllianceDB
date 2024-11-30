#!/bin/bash

# the script must be run in hashing/scripts !!!

exp_dir="../../data1/xtra"
exp_secction="PROFILE_MICRO" # sections to run, seperated by ','
ALGOS="SHJ_Shuffle_P_BATCHED" # algorithms to run, seperated by ','
compile=${1:-1} #enable compiling.
Threads=${2:-8}
'''
currently suport
choices of experiment sections:
APP_BENCH: run the four benchmark
MICRO_BENCH: run the synthetic benchmark
PROFILE_MICRO: run the synthetic benchmark and profile
SCALE_STUDY: run the synthetic benchmark with 1, 2, 4, 8 threads
'''

'''
{"SHJ_st",      SHJ_st}, /* Symmetric hash join single_thread*/
{"SHJ_JM_P",    SHJ_JM_P}, /* Symmetric hash join JM Model, Partition*/
{"SHJ_JM_P_BATCHED",SHJ_JM_P_BATCHED}, /* Symmetric hash join JM Model, Batched*/
{"SHJ_Shuffle_P_BATCHED",SHJ_Shuffle_P_BATCHED},/* Symetric hash join with hash shuffling, batched*/
{"SHJ_JM_NP",   SHJ_JM_NP}, /* Symmetric hash join JM Model, No-Partition*/
{"SHJ_JB_NP",   SHJ_JB_NP}, /* Symmetric hash join JB Model, No-Partition*/
{"SHJ_JBCR_NP", SHJ_JBCR_NP}, /* Symmetric hash join JB CountRound Model, No-Partition*/
{"SHJ_JBCR_P",  SHJ_JBCR_P}, /* Symmetric hash join JB CountRound Model, No-Partition*/
{"SHJ_HS_NP",   SHJ_HS_NP}, /* Symmetric hash join HS Model, No-Partition*/
'''

export PERF_RESULT_DIR=perf_results
export PERF_REPORT_DIR=perf_results/reports
export PERF_LOG_DIR=perf_results/logs
export PERF_DATA_DIR=perf_results/data
export PERF_VIS_DIR=perf_results/visualization
export PERF_TMP_DIR=perf_results/tmp
export PERF_STAT_DIR=perf_results/stats
export FLAME_GRAPH_DIR=../helper/FlameGraph

mkdir -p ${PERF_RESULT_DIR}
mkdir -p ${PERF_REPORT_DIR}
mkdir -p ${PERF_DATA_DIR}
mkdir -p ${PERF_LOG_DIR}
mkdir -p ${PERF_VIS_DIR}
mkdir -p ${PERF_TMP_DIR}
mkdir -p ${PERF_STAT_DIR}

APP_BENCH=0
MICRO_BENCH=0
SCALE_STUDY=0
PROFILE_MICRO=0
PROFILE=0
PROFILE_MEMORY_CONSUMPTION=0
PROFILE_PMU_COUNTERS=0
PROFILE_TOPDOWN=0

eager=1 #enable eager join.
profile_breakdown=0 # disable measure time breakdown


helpFunction()
{
   echo ""
   echo "Usage: $0 -e exp_section -d exp_dir"
   echo -e "\t-e the experiment section you would like to run"
   echo -e "\t-d the experiment results directory"
   exit 1 # Exit script after printing help
}

while getopts "e:d:c:" opt
do
   case "$opt" in
      e ) exp_secction="$OPTARG" ;;
      d ) exp_dir="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

echo "$exp_secction"
echo "$exp_dir"

# run once to prepare dataset and output folders
# download and mv datasets to exp_dir
# wget https://www.dropbox.com/s/64z4xtpyhhmhojp/datasets.tar.gz
# tar -zvxf datasets.tar.gz
# rm datasets.tar.gz
# mkdir -p $exp_dir
# mv datasets $exp_dir


# ## Create directories on your machine.
mkdir -p $exp_dir/results/breakdown/partition_buildsort_probemerge_join
mkdir -p $exp_dir/results/breakdown/partition_only
mkdir -p $exp_dir/results/breakdown/partition_buildsort_only
mkdir -p $exp_dir/results/breakdown/partition_buildsort_probemerge_only
mkdir -p $exp_dir/results/breakdown/allIncludes

mkdir -p $exp_dir/results/figure
mkdir -p $exp_dir/results/gaps
mkdir -p $exp_dir/results/latency
mkdir -p $exp_dir/results/records
mkdir -p $exp_dir/results/timestamps
# copy custom pmu events to experiment dir.
cp pcm* $exp_dir
# copy cpu mappings to exp_dir
cp ../../cpu-mapping.txt $exp_dir
# set all scripts exp dir
sed -i -e "s/exp_dir = .*/exp_dir = "\"${exp_dir//\//\\/}\""/g" *.py

#####################################################
####### Parse Experiment sections need to run #######
#####################################################

IFS=','
for exp_secions_name in $(echo "$exp_secction");
do
    echo "name = $exp_secions_name"
    case "$exp_secions_name" in
      "APP_BENCH")
        APP_BENCH=1
        ;;
      "MICRO_BENCH"):
        MICRO_BENCH=1
        ;;
      "SCALE_STUDY")
        SCALE_STUDY=1
        ;;
      "PROFILE_MICRO")
        PROFILE_MICRO=1
        ;;
      "PROFILE")
        PROFILE=1
        ;;
      "PROFILE_MEMORY_CONSUMPTION")
        PROFILE_MEMORY_CONSUMPTION=1
        ;;
      "PROFILE_PMU_COUNTERS")
        PROFILE_PMU_COUNTERS=1
        ;;
      "PROFILE_TOPDOWN")
        PROFILE_TOPDOWN=1
        ;;
    esac
done

echo "Total EXPS: ${exp_secction}"


##### Set L3 Cache according to your machine.
# sed -i -e "s/#define L3_CACHE_SIZE [[:alnum:]]*/#define L3_CACHE_SIZE $L3_cache_size/g" ../utils/params.h

###### Set experiment dir
sed -i -e "s/#define EXP_DIR .*/#define EXP_DIR "\"${exp_dir//\//\\/}\""/g" ../joins/common_functions.h
sed -i -e "s/#define PERF_COUNTERS/#define NO_PERF_COUNTERS/g" ../utils/perf_counters.h
sed -i -e "s/#define PROFILE_TOPDOWN/#define NO_PROFILE_TOPDOWN/g" ../utils/perf_counters.h
sed -i -e "s/#define PROFILE_MEMORY_CONSUMPTION/#define NO_PROFILE_MEMORY_CONSUMPTION/g" ../utils/perf_counters.h
sed -i -e "s/#define NO_TIMING/#define TIMING/g" ../joins/common_functions.h

###### change cpu-mapping path here, e.g. following changes $exp_dir/cpu-mapping.txt to $exp_dir/cpu-mapping.txt
# sed -i -e "s/\/data1\/xtra\/cpu-mapping.txt/\/data1\/xtra\/cpu-mapping.txt/g" ../affinity/cpu_mapping.h

function compile() {
  if [ $compile != 0 ]; then
    if [ $eager == 0 ] || [ $profile_breakdown == 1 ]; then #to reduce profile overhead, we postpone eager joins during profiling.
      sed -i -e "s/#define EAGER/#define NO_EAGER/g" ../joins/common_functions.h
    else
      sed -i -e "s/#define NO_EAGER/#define EAGER/g" ../joins/common_functions.h
    fi
    cd ..
    cmake . | tail -n +90
    cd scripts
    make -C .. clean -s
    make -C .. -j4 -s
  fi
}

function benchmarkRun() {
  #####native execution
  echo "==benchmark:$benchmark -a $algo -t $ts -w $WINDOW_SIZE -r $RSIZE -s $SSIZE -R $RPATH -S $SPATH -J $RKEY -K $SKEY -L $RTS -M $STS -n $Threads -B 1 -t 1 -I $id -[ $progress_step -] $merge_step -G $group -g $gap -o $exp_dir/results/breakdown/profile_$id.txt =="
  echo 3 >/proc/sys/vm/drop_caches
  ../hashing -a $algo -t $ts -w $WINDOW_SIZE -r $RSIZE -s $SSIZE -R $RPATH -S $SPATH -J $RKEY -K $SKEY -L $RTS -M $STS -n $Threads -B 1 -t 1 -I $id -[ $progress_step -] $merge_step -G $group -g $gap -o $exp_dir/results/breakdown/profile_$id.txt
  if [[ $? -eq 139 ]]; then echo "oops, sigsegv" exit -1; fi
}

function KimRun() {
  #####native execution
  echo "==KIM benchmark:$benchmark -a $algo -t $ts -w $WINDOW_SIZE -e $STEP_SIZE -q $STEP_SIZE_S -l $INTERVAL -d $distrbution -z $skew -D $TS_DISTRIBUTION -Z $ZIPF_FACTOR -n $Threads -I $id -W $FIXS -[ $progress_step -] $merge_step -G $group -P $DD -g $gap =="
  echo 3 >/proc/sys/vm/drop_caches
  ../hashing -a $algo -t $ts -w $WINDOW_SIZE -e $STEP_SIZE -q $STEP_SIZE_S -l $INTERVAL -d $distrbution -z $skew -D $TS_DISTRIBUTION -Z $ZIPF_FACTOR -n $Threads -I $id -W $FIXS -[ $progress_step -] $merge_step -G $group -P $DD -g $gap
  if [[ $? -eq 139 ]]; then echo "oops, sigsegv" exit -1; fi
}

function KimProfStatRun() {
  #####native execution
  echo "==KIM benchmark:$benchmark -a $algo -t $ts -w $WINDOW_SIZE -e $STEP_SIZE -q $STEP_SIZE_S -l $INTERVAL -d $distrbution -z $skew -D $TS_DISTRIBUTION -Z $ZIPF_FACTOR -n $Threads -I $id -W $FIXS -[ $progress_step -] $merge_step -G $group -P $DD -g $gap =="
  # Clear cache, avoid cache interference of previous round. Need sudo
  echo 3 >/proc/sys/vm/drop_caches

  # perf record -F 256 -g --call-graph dwarf -o ${PERF_DATA_DIR}/${algo}_${benchmark}_THREAD${Threads}_perf_record_result.data  \
  # ../hashing -a $algo -t $ts -w $WINDOW_SIZE -e $STEP_SIZE -q $STEP_SIZE_S -l $INTERVAL -d $distrbution -z $skew -D $TS_DISTRIBUTION -Z $ZIPF_FACTOR -n $Threads -I $id -W $FIXS -[ $progress_step -] $merge_step -G $group -P $DD -g $gap
  # > ${PERF_LOG_DIR}/${algo}_${benchmark}_THREAD${Threads}_record_run_log.txt

  # after obtaining the results, run the command below to dump binary stack samples
  echo "perf stat -d -o ${PERF_STAT_DIR}/${algo}_${benchmark}_THREAD${Threads}_perf_result.txt  \
    ../hashing -a $algo -t $ts -w $WINDOW_SIZE -e $STEP_SIZE -q $STEP_SIZE_S -l $INTERVAL -d $distrbution -z $skew -D $TS_DISTRIBUTION -Z $ZIPF_FACTOR -n $Threads -I $id -W $FIXS -[ $progress_step -] $merge_step -G $group -P $DD -g $gap \
    > ${PERF_LOG_DIR}/${algo}_${benchmark}_THREAD${Threads}_run_log.txt"

  perf stat -d -d -o ${PERF_STAT_DIR}/${algo}_${benchmark}_THREAD${Threads}_perf_result.txt  \
    ../hashing -a $algo -t $ts -w $WINDOW_SIZE -e $STEP_SIZE -q $STEP_SIZE_S -l $INTERVAL -d $distrbution -z $skew -D $TS_DISTRIBUTION -Z $ZIPF_FACTOR -n $Threads -I $id -W $FIXS -[ $progress_step -] $merge_step -G $group -P $DD -g $gap \
    > ${PERF_LOG_DIR}/${algo}_${benchmark}_THREAD${Threads}_run_log.txt

  if [[ $? -eq 139 ]]; then echo "oops, sigsegv" exit -1; fi
}

function KimFlameGraphVisualizeRun() {
  #####native execution
  echo "==KIM benchmark:$benchmark -a $algo -t $ts -w $WINDOW_SIZE -e $STEP_SIZE -q $STEP_SIZE_S -l $INTERVAL -d $distrbution -z $skew -D $TS_DISTRIBUTION -Z $ZIPF_FACTOR -n $Threads -I $id -W $FIXS -[ $progress_step -] $merge_step -G $group -P $DD -g $gap =="
  # Clear cache, avoid cache interference of previous round. Need sudo
  echo 3 >/proc/sys/vm/drop_caches

  perf record -F 256 -g --call-graph dwarf -o ${PERF_DATA_DIR}/${algo}_${benchmark}_THREAD${Threads}_perf_record_result.data  \
  ../hashing -a $algo -t $ts -w $WINDOW_SIZE -e $STEP_SIZE -q $STEP_SIZE_S -l $INTERVAL -d $distrbution -z $skew -D $TS_DISTRIBUTION -Z $ZIPF_FACTOR -n $Threads -I $id -W $FIXS -[ $progress_step -] $merge_step -G $group -P $DD -g $gap
  > ${PERF_LOG_DIR}/${algo}_${benchmark}_THREAD${Threads}_record_run_log.txt

  # # after obtaining the results, run the command below to dump binary stack samples
  echo "perf report --stdio -i ${PERF_DATA_DIR}/${algo}_${benchmark}_THREAD${Threads}_perf_record_result.data 
  > ${PERF_REPORT_DIR}/${algo}_${benchmark}_THREAD${Threads}_report.txt"
  perf report --stdio -n -i ${PERF_DATA_DIR}/${algo}_${benchmark}_THREAD${Threads}_perf_record_result.data > ${PERF_REPORT_DIR}/${algo}_${benchmark}_THREAD${Threads}_report.txt
  
  # perf script > ../../../FlameGraph/out.perf
  # ../../../FlameGraph/stackcollapse-perf.pl out.perf > out.folded
  # ../../../FlameGraph/flamegraph.pl out.kern_folded > kernel.svg
  perf script -i ${PERF_DATA_DIR}/${algo}_${benchmark}_THREAD${Threads}_perf_record_result.data > ${PERF_TMP_DIR}/__tmp__${algo}_${benchmark}_THREAD${Threads}_perf_script_result_dump.txt
  ${FLAME_GRAPH_DIR}/stackcollapse-perf.pl ${PERF_TMP_DIR}/__tmp__${algo}_${benchmark}_THREAD${Threads}_perf_script_result_dump.txt > ${PERF_TMP_DIR}/__tmp__stack_fold_${algo}_${benchmark}_THREAD${Threads}_perf_script_result_dump.txt
  ${FLAME_GRAPH_DIR}/flamegraph.pl ${PERF_TMP_DIR}/__tmp__stack_fold_${algo}_${benchmark}_THREAD${Threads}_perf_script_result_dump.txt > ${PERF_VIS_DIR}/${algo}_${benchmark}_THREAD${Threads}_flame_graph.svg

  if [[ $? -eq 139 ]]; then echo "oops, sigsegv" exit -1; fi
}

########################################################
####### Config parameters for benchmark workload #######
########################################################

function SetStockParameters() { #matches: 15598112. #inputs= 60527 + 77227
  ts=1 # stream case
  WINDOW_SIZE=1000
  RSIZE=60527
  SSIZE=77227
  RPATH=$exp_dir/datasets/stock/cj_1000ms_1t.txt
  SPATH=$exp_dir/datasets/stock/sb_1000ms_1t.txt
  RKEY=0
  SKEY=0
  RTS=1
  STS=1
  gap=15595
}

function SetRovioParameters() { #matches: 87856849382 #inputs= 2873604 + 2873604
  ts=1 # stream case
  WINDOW_SIZE=1000
  RSIZE=2873604
  SSIZE=2873604
  RPATH=$exp_dir/datasets/rovio/1000ms_1t.txt
  SPATH=$exp_dir/datasets/rovio/1000ms_1t.txt
  RKEY=0
  SKEY=0
  RTS=3
  STS=3
  gap=87856849
}

function SetYSBParameters() { #matches: 10000000. #inputs= 1000 + 10000000
  ts=1 # stream case
  WINDOW_SIZE=1000
  RSIZE=1000
  SSIZE=10000000
  RPATH=$exp_dir/datasets/YSB/campaigns_id.txt
  SPATH=$exp_dir/datasets/YSB/ad_events.txt
  RKEY=0
  SKEY=0
  RTS=0
  STS=1
  gap=10000
}

function SetDEBSParameters() { #matches: 251033140 #inputs= 1000000 + 1000000
  ts=1 # stream case
  WINDOW_SIZE=0
  RSIZE=1000000 #1000000
  SSIZE=1000000 #1000000
  RPATH=$exp_dir/datasets/DEBS/posts_key32_partitioned.csv
  SPATH=$exp_dir/datasets/DEBS/comments_key32_partitioned.csv
  RKEY=0
  SKEY=0
  RTS=0
  STS=0
  gap=251033
}

DEFAULT_WINDOW_SIZE=100 #(ms) -- 0.1 seconds -- HS is too slow.
DEFAULT_STEP_SIZE=12800 # |tuples| per ms. -- 128K per seconds. ## this controls the guranalrity of input stream.
function ResetParameters() {
  TS_DISTRIBUTION=0                # uniform time distribution
  ZIPF_FACTOR=0                    # uniform time distribution
  distrbution=0                    # unique
  skew=0                           # uniform key distribution
  INTERVAL=1                       # interval of 1. always..
  STEP_SIZE=$DEFAULT_STEP_SIZE     # arrival rate = 1000 / ms
  WINDOW_SIZE=$DEFAULT_WINDOW_SIZE # MS rel size = window_size / interval * step_size.
  STEP_SIZE_S=128000               # let S has the same arrival rate of R.
  FIXS=1
  ts=1 # stream case
  # Threads=8
  progress_step=20
  merge_step=16 #not in use.
  group=1
  gap=12800
  DD=1
  sed -i -e "s/scalarflag [[:alnum:]]*/scalarflag 0/g" ../helper/sort_common.h
  sed -i -e "s/NUM_RADIX_BITS [[:alnum:]]*/NUM_RADIX_BITS 8/g" ../joins/prj_params.h
}

#################################################
####### Config shj source file dependency #######
#################################################

function ALL_ON() {
  sed -i -e "s/#define NO_JOIN/#define JOIN/g" ../joins/common_functions.h
  sed -i -e "s/#define NO_MERGE/#define MERGE/g" ../joins/common_functions.h
  sed -i -e "s/#define NO_MATCH/#define MATCH/g" ../joins/common_functions.h
  sed -i -e "s/#define NO_WAIT/#define WAIT/g" ../joins/common_functions.h
  sed -i -e "s/#define NO_OVERVIEW/#define OVERVIEW/g" ../joins/common_functions.h
}


function NORMAL() {
  sed -i -e "s/#define NO_TIMING/#define TIMING/g" ../joins/common_functions.h #disable time measurement
  sed -i -e "s/#define PERF_COUNTERS/#define NO_PERF_COUNTERS/g" ../utils/perf_counters.h
  sed -i -e "s/#define PROFILE_TOPDOWN/#define NO_PROFILE_TOPDOWN/g" ../joins/common_functions.h
  sed -i -e "s/#define PROFILE_MEMORY_CONSUMPTION/#define NO_PROFILE_MEMORY_CONSUMPTION/g" ../joins/common_functions.h
}

function RUNALL() {
  ALL_ON
  compile
  benchmarkRun
}

function KIMRUN() {
  ALL_ON
  compile
  echo "ALL_ON"
  KimRun
}

function KIMRUNPROF() {
  ALL_ON
  compile
  KimFlameGraphVisualizeRun
  KimProfStatRun
}

#compile once if specified
compile
# Configurable variables
# Generate a timestamp
timestamp=$(date +%Y%m%d-%H%M)
output=test_shj_$timestamp.txt

## APP benchmark.
#APP_BENCH=0
if [ $APP_BENCH == 1 ]; then
  NORMAL
  #compile depends on whether we want to profile.
  compile=0
  for benchmark in "Stock"; do # "Stock" "Rovio" "YSB" "DEBS"
    IFS=','
    for algo in $(echo "$ALGOS"); do
      case "$benchmark" in
      "Stock")
        id=38
        ResetParameters
        SetStockParameters
        RUNALL
        ;;
      "Rovio")
        id=39
        ResetParameters
        SetRovioParameters
        RUNALL
        ;;
      "YSB")
        id=40
        ResetParameters
        SetYSBParameters
        RUNALL
        ;;
      "DEBS")
        id=41
        ResetParameters
        SetDEBSParameters
        RUNALL
        ;;
      esac
    done
  done
fi

## MICRO benchmark.
#MICRO_BENCH=0
if [ $MICRO_BENCH == 1 ]; then
  NORMAL
  compile=0
  # for benchmark in "AR" "RAR" "AD" "KD" "WS" "DD"; do #
  for benchmark in "FF"; do
    IFS=','
    for algo in $(echo "$ALGOS"); do
      case "$benchmark" in
      "FF")
        id=5
        ## Figure 2
        ResetParameters
        FIXS=1
        ts=1 # stream case
        WINDOW_SIZE=3000
        # step size should be bigger than nthreads
        STEP_SIZE=6400
        STEP_SIZE_S=6400
        #        WINDOW_SIZE=$(expr $DEFAULT_WINDOW_SIZE \* $DEFAULT_STEP_SIZE / $STEP_SIZE) #ensure relation size is the same.
        echo relation size R is $(expr $WINDOW_SIZE / $INTERVAL \* $STEP_SIZE)
        echo relation size S is $(expr $WINDOW_SIZE / $INTERVAL \* $STEP_SIZE_S)
        gap=$(($STEP_SIZE / 500 * $WINDOW_SIZE))
        KIMRUN
        let "id++"
        ;;
      esac
    done
  done
fi


## MICRO PROFILE.
if [ $PROFILE_MICRO == 1 ]; then
  NORMAL
  compile=0
  IFS=','
  benchmark = "FF"
  for algo in $(echo "$ALGOS"); do
    ResetParameters
    FIXS=1
    ts=1 # stream case
    WINDOW_SIZE=3000
    # step size should be bigger than nthreads
    STEP_SIZE=6400
    STEP_SIZE_S=6400
    echo relation size R is $(expr $WINDOW_SIZE / $INTERVAL \* $STEP_SIZE)
    echo relation size S is $(expr $WINDOW_SIZE / $INTERVAL \* $STEP_SIZE_S)
    gap=$(($STEP_SIZE / 500 * $WINDOW_SIZE))
    KIMRUNPROF
  done
fi


## SCLAE STUDY
#SCALE_STUDY=0
if [ $SCALE_STUDY == 1 ]; then
  NORMAL
  profile_breakdown=0
  compile=0
  for algo in $(echo "$ALGOS"); do
    ResetParameters
    FIXS=1
    ts=1 # stream case
    WINDOW_SIZE=3000
    STEP_SIZE=6400
    STEP_SIZE_S=6400
    gap=$(($STEP_SIZE / 500 * $WINDOW_SIZE))
    for Threads in 1 2 4 8 16; do
      echo Running thread no. $Threads
      KIMRUN
    done
  done
fi

# bash draw.sh
# python3 jobdone.py
echo "SHJ Experiments All Done"
