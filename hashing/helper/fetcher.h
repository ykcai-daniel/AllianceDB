//
// Created by Shuhao Zhang on 1/11/19.
//

#ifndef ALLIANCEDB_FETCHER_H
#define ALLIANCEDB_FETCHER_H

#include "../utils/types.h"
#include "../joins/common_functions.h"
#include <stdio.h>
#include <unistd.h>
#include <chrono>
#include <thread>

using namespace std::chrono;

enum fetcher {
    type_HS_NP_Fetcher, type_JM_NP_Fetcher, type_JB_NP_Fetcher, type_PMJ_HS_NP_Fetcher
};


struct fetch_t {
    fetch_t(fetch_t *fetch);

    fetch_t();

    tuple_t *tuple = nullptr;//normal tuples.

    tuple_t *fat_tuple = nullptr;//used for PMJ only.

    int fat_tuple_size = 0;

    bool ISTuple_R;//whether this tuple from input R (true) or S (false).

    bool ack = false;//whether this is just a message. Used in HS model.
};

//thread local structure
struct t_state {
    int start_index_R = 0;//configure pointer of start reading point.
    int end_index_R = 0;//configure pointer of end reading point.
    int start_index_S = 0;//configure pointer of start reading point.
    int end_index_S = 0;//configure pointer of end reading point.
    //read R/S alternatively.
    bool IsTupleR;
    fetch_t fetch;
};

class baseFetcher {
public:
    virtual fetch_t *next_tuple(int tid) = 0;

    relation_t *relR;//input relation
    relation_t *relS;//input relation

    milliseconds *RdataTime;
    milliseconds *SdataTime;
    milliseconds fetchStartTime = (milliseconds)-1;

    t_state *state;

    milliseconds RtimeGap(milliseconds *time) {
        if (fetchStartTime == (milliseconds) -1) {
            fetchStartTime = (milliseconds) curtick();
            return (milliseconds)0;
        } else {
            return (*time - *RdataTime) - ((milliseconds) curtick() - fetchStartTime);//if it's positive, the tuple is not ready yet.
        }
    }

    milliseconds StimeGap(milliseconds *time) {
        if (fetchStartTime == (milliseconds) -1) {
            fetchStartTime = (milliseconds) curtick();
            return (milliseconds) 0;
        } else {
            return (*time - *SdataTime) - ((milliseconds) curtick() - fetchStartTime);//if it's positive, the tuple is not ready yet.
        }
    }
    void Rproceed(milliseconds *time) {
        if (fetchStartTime == (milliseconds) -1) {
            fetchStartTime = (milliseconds) curtick();
        } else {
            sleep((((milliseconds) curtick() - fetchStartTime) - (*time - *RdataTime)).count());
        }
    }

    void Sproceed(milliseconds *time) {
        if (fetchStartTime == (milliseconds) -1) {
            fetchStartTime = (milliseconds) curtick();
        } else {
            sleep((((milliseconds) curtick() - fetchStartTime) - (*time - *SdataTime)).count());
        }
    }

    virtual bool finish() = 0;

    baseFetcher(relation_t *relR, relation_t *relS) {
        this->relR = relR;
        this->relS = relS;
        RdataTime = relR->payload->ts;
        SdataTime = relS->payload->ts;
    }
};

inline bool last_thread(int i, int nthreads) {
    return i == (nthreads - 1);
}

class PMJ_HS_NP_Fetcher : public baseFetcher {
public:
    fetch_t *next_tuple(int tid);

    bool finish() {
        return false;//should not be called.
    }

    /**
     * Initialization
     * @param nthreads
     * @param relR
     * @param relS
     */
    PMJ_HS_NP_Fetcher(int nthreads, relation_t *relR, relation_t *relS, int i)
            : baseFetcher(relR, relS) {
        state = new t_state();

        //let first and last thread to read two streams.
        if (i == 0) {
            state->IsTupleR = true;
            /* replicate relR to thread 0 */
            state->start_index_R = 0;
            state->end_index_R = relR->num_tuples;
        }
        if (i == nthreads - 1) {
            /* replicate relS to thread [nthread-1] */
            state->start_index_S = 0;
            state->end_index_S = relS->num_tuples;
        }

        DEBUGMSG("TID:%d, R: start_index:%d, end_index:%d\n", i, state->start_index_R, state->end_index_R);
        DEBUGMSG("TID:%d, S: start_index:%d, end_index:%d\n", i, state->start_index_S, state->end_index_S);
    }
};

class HS_NP_Fetcher : public baseFetcher {
public:
    fetch_t *next_tuple(int tid);

    bool finish() {
        return false;//should not be called.
    }

    /**
     * Initialization
     * @param nthreads
     * @param relR
     * @param relS
     */
    HS_NP_Fetcher(int nthreads, relation_t *relR, relation_t *relS, int i)
            : baseFetcher(relR, relS) {
        state = new t_state();

        //let first and last thread to read two streams.
        if (i == 0) {
            state->IsTupleR = true;
            /* replicate relR to thread 0 */
            state->start_index_R = 0;
            state->end_index_R = relR->num_tuples;
        }
        if (i == nthreads - 1) {
            /* replicate relS to thread [nthread-1] */
            state->start_index_S = 0;
            state->end_index_S = relS->num_tuples;
        }

        DEBUGMSG("TID:%d, R: start_index:%d, end_index:%d\n", i, state->start_index_R, state->end_index_R);
        DEBUGMSG("TID:%d, S: start_index:%d, end_index:%d\n", i, state->start_index_S, state->end_index_S);
    }
};

class JM_P_Fetcher : public baseFetcher {
public:
    fetch_t *next_tuple(int tid);

    bool finish() {
        return state->start_index_R == state->end_index_R
               && state->start_index_S == state->end_index_S;
    }

    /**
     * Initialization
     * @param nthreads
     * @param relR
     * @param relS
     */
    JM_P_Fetcher(int nthreads, relation_t *relR, relation_t *relS, int i)
            : baseFetcher(relR, relS) {
        state = new t_state[nthreads];

        int numSthr = relS->num_tuples / nthreads;//replicate R, partition S.


        state->IsTupleR = true;
        /* replicate relR for next thread */
        state->start_index_R = 0;
        state->end_index_R = relR->num_tuples;

        /* assign part of the relS for next thread */
        state->start_index_S = numSthr * i;
        state->end_index_S = (last_thread(i, nthreads)) ? relS->num_tuples : numSthr * (i + 1);

        DEBUGMSG("TID:%d, R: start_index:%d, end_index:%d\n", i, state->start_index_R, state->end_index_R);
        DEBUGMSG("TID:%d, S: start_index:%d, end_index:%d\n", i, state->start_index_S, state->end_index_S);


    }
};

class JM_NP_Fetcher : public baseFetcher {
public:
    fetch_t *next_tuple(int tid);

    bool finish() {
        return state->start_index_R == state->end_index_R
               && state->start_index_S == state->end_index_S;
    }

    /**
     * Initialization
     * @param nthreads
     * @param relR
     * @param relS
     */
    JM_NP_Fetcher(int nthreads, relation_t *relR, relation_t *relS, int i)
            : baseFetcher(relR, relS) {
        state = new t_state();

        int numSthr = relS->num_tuples / nthreads;//replicate R, partition S.

        state->IsTupleR = true;
        /* replicate relR for next thread */
        state->start_index_R = 0;
        state->end_index_R = relR->num_tuples;

        /* assign part of the relS for next thread */
        state->start_index_S = numSthr * i;
        state->end_index_S = (last_thread(i, nthreads)) ? relS->num_tuples : numSthr * (i + 1);

        DEBUGMSG("TID:%d, R: start_index:%d, end_index:%d\n", i, state->start_index_R, state->end_index_R);
        DEBUGMSG("TID:%d, S: start_index:%d, end_index:%d\n", i, state->start_index_S, state->end_index_S);
    }
};

class JB_NP_Fetcher : public baseFetcher {
public:
    fetch_t *next_tuple(int tid);

    bool finish() {
        return state->start_index_R == state->end_index_R
               && state->start_index_S == state->end_index_S;
    }

    JB_NP_Fetcher(int nthreads, relation_t *relR, relation_t *relS, int i)
            : baseFetcher(relR, relS) {
        state = new t_state[nthreads];
        int numRthr = relR->num_tuples / nthreads;// partition R,
        int numSthr = relS->num_tuples / nthreads;// partition S.

        state->IsTupleR = true;
        /* assign part of the relR for next thread */
        state->start_index_R = numRthr * i;
        state->end_index_R = (last_thread(i, nthreads)) ? relR->num_tuples : numRthr * (i + 1);

        /* assign part of the relS for next thread */
        state->start_index_S = numSthr * i;
        state->end_index_S = (last_thread(i, nthreads)) ? relS->num_tuples : numSthr * (i + 1);

        DEBUGMSG("TID:%d, R: start_index:%d, end_index:%d\n", i, state->start_index_R, state->end_index_R);
        DEBUGMSG("TID:%d, S: start_index:%d, end_index:%d\n", i, state->start_index_S, state->end_index_S);

    }
};

#endif //ALLIANCEDB_FETCHER_H
