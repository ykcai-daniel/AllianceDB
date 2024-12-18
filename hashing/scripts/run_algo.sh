#!/bin/bash
# a lightweighted debugging script for running and profiling one single algo
# the script must be run in hashing/scripts with sudo!!!
# ./run_algo.sh

function SetStockParameters() { #matches: 15598112. #inputs= 60527 + 77227
  DATASET_NAME=STOCK
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
  DATASET_NAME=ROVIO
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
  DATASET_NAME=YSB
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
  DATASET_NAME=DEBS
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

export exp_dir=../../data1/xtra

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

function benchmarkRun() {
  #####native execution
  echo "==benchmark:$benchmark -a $algo -t $ts -w $WINDOW_SIZE -r $RSIZE -s $SSIZE -R $RPATH -S $SPATH -J $RKEY -K $SKEY -L $RTS -M $STS -n $Threads -B 1 -t 1 -I $id -[ $progress_step -] $merge_step -G $group -g $gap -o $exp_dir/results/breakdown/profile_$id.txt =="
  # Clear cache, avoid cache interference of previous round. Need sudo
  echo 3 >/proc/sys/vm/drop_caches
  echo "../hashing -a $algo -t $ts -w $WINDOW_SIZE -r $RSIZE -s $SSIZE -R $RPATH -S $SPATH -J $RKEY -K $SKEY -L $RTS -M $STS -n $Threads -B 1 -t 1 -I $id -[ $progress_step -] $merge_step -G $group -g $gap -o $exp_dir/results/breakdown/profile_$id.txt"
  ../hashing -a $algo -t $ts -w $WINDOW_SIZE -r $RSIZE -s $SSIZE -R $RPATH -S $SPATH -J $RKEY -K $SKEY -L $RTS -M $STS -n $Threads -B 1 -t 1 -I $id -[ $progress_step -] $merge_step -G $group -g $gap -o $exp_dir/results/breakdown/profile_$id.txt
  if [[ $? -eq 139 ]]; then echo "oops, sigsegv" exit -1; fi
}

function benchmarkPerfRun() {
  #####native execution
  echo "==benchmark:$benchmark -a $algo -t $ts -w $WINDOW_SIZE -r $RSIZE -s $SSIZE -R $RPATH -S $SPATH -J $RKEY -K $SKEY -L $RTS -M $STS -n $Threads -B 1 -t 1 -I $id -[ $progress_step -] $merge_step -G $group -g $gap -o $exp_dir/results/breakdown/profile_$id.txt =="
  # Clear cache, avoid cache interference of previous round. Need sudo
  echo 3 >/proc/sys/vm/drop_caches
  echo "../hashing -a $algo -t $ts -w $WINDOW_SIZE -r $RSIZE -s $SSIZE -R $RPATH -S $SPATH -J $RKEY -K $SKEY -L $RTS -M $STS -n $Threads -B 1 -t 1 -I $id -[ $progress_step -] $merge_step -G $group -g $gap -o $exp_dir/results/breakdown/profile_$id.txt"
  #perf stat -e CPU_CLK_UNHALTED.THREAD,IDQ_UOPS_NOT_DELIVERED.CORE,UOPS_ISSUED.ANY,UOPS_RETIRED.RETIRE_SLOTS,INT_MISC.RECOVERY_CYCLES,RESOURCE_STALLS.SB \
  perf stat -d -o ${PERF_RESULT_DIR}/${algo}_${DATASET_NAME}_THREAD${THREAD_ARG}_perf_result.txt  \
    ../hashing -a $algo -t $ts -w $WINDOW_SIZE -r $RSIZE -s $SSIZE -R $RPATH -S $SPATH -J $RKEY -K $SKEY -L $RTS -M $STS -n $Threads -B 1 -t 1 -I $id -[ $progress_step -] $merge_step -G $group -g $gap \
    > ${PERF_RESULT_DIR}/${algo}_${DATASET_NAME}_THREAD${THREAD_ARG}_run_log.txt

  if [[ $? -eq 139 ]]; then echo "oops, sigsegv" exit -1; fi
}

export FLAME_GRAPH_DIR=FlameGraph

function benchmarkFlameGraphRun() {
  echo "==benchmark:$benchmark -a $algo -t $ts -w $WINDOW_SIZE -r $RSIZE -s $SSIZE -R $RPATH -S $SPATH -J $RKEY -K $SKEY -L $RTS -M $STS -n $Threads -B 1 -t 1 -I $id -[ $progress_step -] $merge_step -G $group -g $gap -o $exp_dir/results/breakdown/profile_$id.txt =="
  # Clear cache, avoid cache interference of previous round. Need sudo
  echo 3 >/proc/sys/vm/drop_caches
  echo "../hashing -a $algo -t $ts -w $WINDOW_SIZE -r $RSIZE -s $SSIZE -R $RPATH -S $SPATH -J $RKEY -K $SKEY -L $RTS -M $STS -n $Threads -B 1 -t 1 -I $id -[ $progress_step -] $merge_step -G $group -g $gap -o $exp_dir/results/breakdown/profile_$id.txt"
  #perf stat -e CPU_CLK_UNHALTED.THREAD,IDQ_UOPS_NOT_DELIVERED.CORE,UOPS_ISSUED.ANY,UOPS_RETIRED.RETIRE_SLOTS,INT_MISC.RECOVERY_CYCLES,RESOURCE_STALLS.SB \
  perf record -F 120 -g --call-graph dwarf -o ${PERF_RESULT_DIR}/${algo}_${DATASET_NAME}_THREAD${THREAD_ARG}_perf_record_result.data  \
    ../hashing -a $algo -t $ts -w $WINDOW_SIZE -r $RSIZE -s $SSIZE -R $RPATH -S $SPATH -J $RKEY -K $SKEY -L $RTS -M $STS -n $Threads -B 1 -t 1 -I $id -[ $progress_step -] $merge_step -G $group -g $gap \
    > ${PERF_RESULT_DIR}/${algo}_${DATASET_NAME}_THREAD${THREAD_ARG}_record_run_log.txt

  # after obtaining the results, run the command below to dump binary stack samples into human-readable format
  # perf report --stdio -n -i ${PERF_RESULT_DIR}/${algo}_${DATASET_NAME}_THREAD${THREAD_ARG}_perf_record_result.data > ${PERF_RESULT_DIR}/${algo}_${DATASET_NAME}_THREAD${THREAD_ARG}_perf_record_result_dump.txt

  # use perf script to dump the binary results for flame graph visualization
  perf script -i ${PERF_RESULT_DIR}/${algo}_${DATASET_NAME}_THREAD${THREAD_ARG}_perf_record_result.data > ${PERF_RESULT_DIR}/__tmp__${algo}_${DATASET_NAME}_THREAD${THREAD_ARG}_perf_script_result_dump.txt
  ${FLAME_GRAPH_DIR}/stackcollapse-perf.pl ${PERF_RESULT_DIR}/__tmp__${algo}_${DATASET_NAME}_THREAD${THREAD_ARG}_perf_script_result_dump.txt > ${PERF_RESULT_DIR}/__tmp__stack_fold_${algo}_${DATASET_NAME}_THREAD${THREAD_ARG}_perf_script_result_dump.txt
  ${FLAME_GRAPH_DIR}/flamegraph.pl ${PERF_RESULT_DIR}/__tmp__stack_fold_${algo}_${DATASET_NAME}_THREAD${THREAD_ARG}_perf_script_result_dump.txt > ${algo}_${DATASET_NAME}_THREAD${THREAD_ARG}_flame_graph.svg


  if [[ $? -eq 139 ]]; then echo "oops, sigsegv" exit -1; fi

}


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



# {"SHJ_st",      SHJ_st}, /* Symmetric hash join single_thread*/
# {"SHJ_JM_P",    SHJ_JM_P}, /* Symmetric hash join JM Model, Partition*/
# {"SHJ_JM_P_BATCHED",SHJ_JM_P_BATCHED}, /* Symmetric hash join JM Model, Batched*/
# {"SHJ_Shuffle_P_BATCHED",SHJ_Shuffle_P_BATCHED},/* Symetric hash join with hash shuffling, batched*/
# {"SHJ_JM_NP",   SHJ_JM_NP}, /* Symmetric hash join JM Model, No-Partition*/
# {"SHJ_JB_NP",   SHJ_JB_NP}, /* Symmetric hash join JB Model, No-Partition*/
# {"SHJ_JBCR_NP", SHJ_JBCR_NP}, /* Symmetric hash join JB CountRound Model, No-Partition*/
# {"SHJ_JBCR_P",  SHJ_JBCR_P}, /* Symmetric hash join JB CountRound Model, No-Partition*/
# {"SHJ_HS_NP",   SHJ_HS_NP}, /* Symmetric hash join HS Model, No-Partition*/

COMPILE_ARG=${1:-0}
# We focus on SHJ_JM_P, SHJ_JM_P_BATCHED, SHJ_Shuffle_P_BATCHED, SHJ_JB_NP
ALGO_ARG=${2:-SHJ_Shuffle_P_BATCHED}
THREAD_ARG=${3:-4}

echo "Compile: ${COMPILE_ARG}. Running algo: ${ALGO_ARG} with thread ${THREAD_ARG}"
export PERF_RESULT_DIR=perf_results
mkdir -p ${PERF_RESULT_DIR}

ALL_ON
compile=${COMPILE_ARG}
compile
ResetParameters
# dataset is hardcoded
SetYSBParameters
algo=${ALGO_ARG}
Threads=${THREAD_ARG}
benchmarkFlameGraphRun
