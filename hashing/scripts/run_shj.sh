#!/bin/bash

# the script must be run in hashing/scripts !!!

exp_dir="../../data1/xtra"
exp_secction="MICRO_BENCH"
# exp_secction="APP_BENCH,MICRO_BENCH,SCALE_STUDY,PROFILE_MICRO,PROFILE,PROFILE_MEMORY_CONSUMPTION,PROFILE_PMU_COUNTERS"
'''
choices of experiment sections:
APP_BENCH: run the four benchmark for SHJ_JM_NP, SHJ_JM_P, SHJ_JM_P_BATCHED
MICRO_BENCH: run the synthetic benchmark for SHJ_JM_NP, SHJ_JM_P, SHJ_JM_P_BATCHED
PROFILE_MEMORY_CONSUMPTION: run the four benchmark for SHJ_JM_P, SHJ_JM_P_BATCHED with memory consumption

SCALE_STUDY: run the four benchmark for SHJ_JM_P, SHJ_JM_P_BATCHED with 1, 2, 4, 8 threads
PROFILE: profile cache misses
PROFILE_PMU_COUNTERS: profile PMU counters using pcm
PROFILE_TOPDOWN: profile intel topdown performance metrics using perf/pcm
'''

APP_BENCH=0
MICRO_BENCH=0
SCALE_STUDY=0
PROFILE_MICRO=0
PROFILE=0
PROFILE_MEMORY_CONSUMPTION=0
PROFILE_PMU_COUNTERS=0
PROFILE_TOPDOWN=0

compile=1 #enable compiling.
eager=1 #enable eager join.
profile_breakdown=1 # disable measure time breakdown


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
# mkdir -p $exp_dir/results/breakdown/partition_buildsort_probemerge_join
# mkdir -p $exp_dir/results/breakdown/partition_only
# mkdir -p $exp_dir/results/breakdown/partition_buildsort_only
# mkdir -p $exp_dir/results/breakdown/partition_buildsort_probemerge_only
# mkdir -p $exp_dir/results/breakdown/allIncludes

# mkdir -p $exp_dir/results/figure
# mkdir -p $exp_dir/results/gaps
# mkdir -p $exp_dir/results/latency
# mkdir -p $exp_dir/results/records
# mkdir -p $exp_dir/results/timestamps
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
  # echo 3 >/proc/sys/vm/drop_caches
  ../hashing -a $algo -t $ts -w $WINDOW_SIZE -r $RSIZE -s $SSIZE -R $RPATH -S $SPATH -J $RKEY -K $SKEY -L $RTS -M $STS -n $Threads -B 1 -t 1 -I $id -[ $progress_step -] $merge_step -G $group -g $gap -o $exp_dir/results/breakdown/profile_$id.txt
  if [[ $? -eq 139 ]]; then echo "oops, sigsegv" exit -1; fi
}

function KimRun() {
  #####native execution
  echo "==KIM benchmark:$benchmark -a $algo -t $ts -w $WINDOW_SIZE -e $STEP_SIZE -q $STEP_SIZE_S -l $INTERVAL -d $distrbution -z $skew -D $TS_DISTRIBUTION -Z $ZIPF_FACTOR -n $Threads -I $id -W $FIXS -[ $progress_step -] $merge_step -G $group -P $DD -g $gap =="
  # echo 3 >/proc/sys/vm/drop_caches
  ../hashing -a $algo -t $ts -w $WINDOW_SIZE -e $STEP_SIZE -q $STEP_SIZE_S -l $INTERVAL -d $distrbution -z $skew -D $TS_DISTRIBUTION -Z $ZIPF_FACTOR -n $Threads -I $id -W $FIXS -[ $progress_step -] $merge_step -G $group -P $DD -g $gap
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
  Threads=8
  progress_step=20
  merge_step=16 #not in use.
  group=2
  gap=12800
  DD=1
  sed -i -e "s/scalarflag [[:alnum:]]*/scalarflag 0/g" ../helper/sort_common.h
  sed -i -e "s/NUM_RADIX_BITS [[:alnum:]]*/NUM_RADIX_BITS 8/g" ../joins/prj_params.h
}


#################################################
####### Config shj source file dependency #######
#################################################


function PARTITION_ONLY() {
  sed -i -e "s/#define JOIN/#define NO_JOIN/g" ../joins/common_functions.h
  sed -i -e "s/#define MERGE/#define NO_MERGE/g" ../joins/common_functions.h
  sed -i -e "s/#define MATCH/#define NO_MATCH/g" ../joins/common_functions.h
  sed -i -e "s/#define WAIT/#define NO_WAIT/g" ../joins/common_functions.h
  sed -i -e "s/#define OVERVIEW/#define NO_OVERVIEW/g" ../joins/common_functions.h
}

function PARTITION_BUILD_SORT() {
  sed -i -e "s/#define NO_JOIN/#define JOIN/g" ../joins/common_functions.h
  sed -i -e "s/#define MERGE/#define NO_MERGE/g" ../joins/common_functions.h
  sed -i -e "s/#define MATCH/#define NO_MATCH/g" ../joins/common_functions.h
  sed -i -e "s/#define WAIT/#define NO_WAIT/g" ../joins/common_functions.h
  sed -i -e "s/#define OVERVIEW/#define NO_OVERVIEW/g" ../joins/common_functions.h
}

function PARTITION_BUILD_SORT_MERGE() {
  sed -i -e "s/#define NO_JOIN/#define JOIN/g" ../joins/common_functions.h
  sed -i -e "s/#define NO_MERGE/#define MERGE/g" ../joins/common_functions.h
  sed -i -e "s/#define MATCH/#define NO_MATCH/g" ../joins/common_functions.h
  sed -i -e "s/#define WAIT/#define NO_WAIT/g" ../joins/common_functions.h
  sed -i -e "s/#define OVERVIEW/#define NO_OVERVIEW/g" ../joins/common_functions.h
}

function PARTITION_BUILD_SORT_MERGE_JOIN() {
  sed -i -e "s/#define NO_JOIN/#define JOIN/g" ../joins/common_functions.h
  sed -i -e "s/#define NO_MERGE/#define MERGE/g" ../joins/common_functions.h
  sed -i -e "s/#define NO_MATCH/#define MATCH/g" ../joins/common_functions.h
  sed -i -e "s/#define WAIT/#define NO_WAIT/g" ../joins/common_functions.h
  sed -i -e "s/#define OVERVIEW/#define NO_OVERVIEW/g" ../joins/common_functions.h
}

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

function SHJBENCHRUN() {
  # PARTITION_ONLY
  # compile
  # echo "PARTITION_ONLY"
  # benchmarkRun

  # PARTITION_BUILD_SORT
  # compile
  # echo "PARTITION_BUILD_SORT"
  # benchmarkRun

  # PARTITION_BUILD_SORT_MERGE_JOIN
  # compile
  # echo "PARTITION_BUILD_SORT_MERGE_JOIN"
  # benchmarkRun

  ALL_ON
  compile
  echo "ALL_ON"
  benchmarkRun
}

function SHJKIMRUN() {
  # PARTITION_ONLY
  # compile
  # KimRun

  # PARTITION_BUILD_SORT
  # compile
  # KimRun

  # PARTITION_BUILD_SORT_MERGE_JOIN
  # compile
  # KimRun

  ALL_ON
  compile
  echo "ALL_ON"
  KimRun
}

function RUNALL() {
  if [ $profile_breakdown == 1 ]; then
      SHJBENCHRUN
  else
    ALL_ON
    compile
    benchmarkRun
  fi
}

function RUNALLMic() {
  if [ $profile_breakdown == 1 ]; then
    SHJKIMRUN
  else
    ALL_ON
    compile
    KimRun
  fi
}

# ALL_ON
# compile=1
# compile
# SetStockParameters
# # algo=SHJ_JM_P_BATCHED
# Threads=4
# benchmarkRun

#compile once by default.
# compile
# Configurable variables
# Generate a timestamp
timestamp=$(date +%Y%m%d-%H%M)
output=test_shj_$timestamp.txt

## APP benchmark.
#APP_BENCH=0
if [ $APP_BENCH == 1 ]; then
  NORMAL
  #compile depends on whether we want to profile.
  for profile_breakdown in 1; do
    compile=1
    for benchmark in "Stock" "Rovio" "YSB" "DEBS"; do # "Stock" "Rovio" "YSB" "DEBS"
      for algo in SHJ_JM_NP SHJ_JM_P SHJ_JM_P_BATCHED; do 
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
  done
fi

## MICRO benchmark.
#MICRO_BENCH=0
if [ $MICRO_BENCH == 1 ]; then
  NORMAL
  profile_breakdown=1        # set to 1 if we want to measure time breakdown!
  compile=$profile_breakdown # compile depends on whether we want to profile.
  # for benchmark in "AR" "RAR" "AD" "KD" "WS" "DD"; do #
  for benchmark in "RAR"; do
    for algo in SHJ_JM_P SHJ_JM_P_BATCHED; do
      case "$benchmark" in
      "RAR")
        id=5
        ## Figure 2
        ResetParameters
        FIXS=1
        echo test relative arrival rate 5 - 13
        ts=1 # stream case
        WINDOW_SIZE=3000
        # step size should be bigger than nthreads
        for STEP_SIZE in 1600; do
          for STEP_SIZE_S in 1600; do
            #        WINDOW_SIZE=$(expr $DEFAULT_WINDOW_SIZE \* $DEFAULT_STEP_SIZE / $STEP_SIZE) #ensure relation size is the same.
            echo relation size R is $(expr $WINDOW_SIZE / $INTERVAL \* $STEP_SIZE)
            echo relation size S is $(expr $WINDOW_SIZE / $INTERVAL \* $STEP_SIZE_S)
            gap=$(($STEP_SIZE / 500 * $WINDOW_SIZE))
            RUNALLMic
            let "id++"
          done
        done
        ;;
      esac
    done
  done
fi

## SCLAE STUDY
#SCALE_STUDY=0
if [ $SCALE_STUDY == 1 ]; then
  NORMAL
  profile_breakdown=0                                                                     #compile depends on whether we want to profile.
  compile=0
  # general benchmark.
  for algo in SHJ_JM_P, SHJ_JM_P_BATCHED; do
    for benchmark in "ScaleStock" "ScaleRovio" "ScaleYSB" "ScaleDEBS"; do #
      case "$benchmark" in
      "ScaleStock")
        id=42
        ResetParameters
        SetStockParameters
        echo test scalability of Stock 42 - 45
        for Threads in 1 2 4 8; do
          RUNALL
          let "id++"
        done
        ;;
      "ScaleRovio")
        id=46
        ResetParameters
        SetRovioParameters
        echo test scalability 46 - 49
        for Threads in 1 2 4 8; do
          RUNALL
          let "id++"
        done
        ;;
      "ScaleYSB")
        id=50
        ResetParameters
        SetYSBParameters
        echo test scalability 50 - 53
        for Threads in 1 2 4 8; do
          RUNALL
          let "id++"
        done
        ;;
      "ScaleDEBS")
        id=54
        ResetParameters
        SetDEBSParameters
        echo test scalability 54 - 57
        for Threads in 1 2 4 8; do
          RUNALL
          let "id++"
        done
        ;;
      esac
    done
  done
fi

## MICRO STUDY

#PROFILE=0 ## Cache misses profiling, please run the program with sudo
if [ $PROFILE == 1 ]; then
  PCM
  profile_breakdown=0 # disable measure time breakdown!
  eager=1             #with eager
  compile=1

  PARTITION_BUILD_SORT_MERGE_JOIN
  compile
  for benchmark in "YSB"; do #"
    id=211
    for algo in SHJ_JM_P SHJ_JM_P_BATCHED; do # ~215 SHJ_JM_P SHJ_JBCR_P
      case "$benchmark" in
      "YSB")
        ResetParameters
        SetYSBParameters
        rm $exp_dir/results/breakdown/profile_$id.txt
        benchmarkRun
        ;;
      esac
      let "id++"
    done
  done
  NORMAL
fi

#PROFILE_MEMORY_CONSUMPTION=1 ## profile memory consumption
if [ $PROFILE_MEMORY_CONSUMPTION == 1 ]; then
  MEM_MEASURE
  profile_breakdown=0
  compile=1
  compile
  for benchmark in "Rovio"; do #"YSB
    id=302
    for algo in SHJ_JM_P SHJ_JM_P_BATCHED; do # SHJ_JM_NP SHJ_JBCR_NP
      case "$benchmark" in
      "Kim")
        ResetParameters
        STEP_SIZE=1280
        STEP_SIZE_S=12800
        WINDOW_SIZE=10000
        rm $exp_dir/results/breakdown/perf_$id.csv
        KimRun
        ;;
      "YSB")
        ResetParameters
        SetYSBParameters
        rm $exp_dir/results/breakdown/perf_$id.txt
        benchmarkRun
        ;;
      "Rovio")
        ResetParameters
        SetRovioParameters
        rm $exp_dir/results/breakdown/perf_$id.txt
        benchmarkRun
        ;;
      esac
      let "id++"
    done
  done
  NORMAL
fi

#PROFILE_PMU_COUNTERS=1 # profile PMU counters using pcm
if [ $PROFILE_PMU_COUNTERS == 1 ]; then
  PCM
  profile_breakdown=0 # disable measure time breakdown!
  ALL_ON # eliminate wait phase
  compile=1
  compile
  for benchmark in "Rovio"; do #"YSB
    id=402
    for algo in SHJ_JM_P SHJ_JM_P_BATCHED; do # SHJ_JM_NP SHJ_JBCR_NP
      case "$benchmark" in
      "Rovio")
        ResetParameters
        SetRovioParameters
        rm $exp_dir/results/breakdown/profile_$id.txt
        PERF_CONF=$exp_dir/pcm.cfg
        benchmarkProfileRun
        PERF_CONF=$exp_dir/pcm2.cfg
        benchmarkProfileRun
        PERF_CONF=""
        benchmarkProfileRun
        ;;
      "YSB")
        ResetParameters
        SetYSBParameters
        rm $exp_dir/results/breakdown/profile_$id.txt
        PERF_CONF=$exp_dir/pcm.cfg
        benchmarkProfileRun
        PERF_CONF=$exp_dir/pcm2.cfg
        benchmarkProfileRun
        PERF_CONF=""
        benchmarkProfileRun
        ;;
      esac
      let "id++"
    done
  done
  NORMAL
fi

# bash draw.sh
# python3 jobdone.py
echo "SHJ Experiments All Done"

