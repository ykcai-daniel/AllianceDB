#pragma once

#include "../utils/types.h"
#include "memory_pool.h"

class Batch{
public:
    constexpr static int default_size=256;
    using array_type=int32_t[default_size];
    using BatchMemoryPool=MemoryPool<array_type ,4096>;

private:

    int size_;


    // debugging field
    int batch_cnt_;

    BatchMemoryPool* memory_pool_;



public:


    array_type * keys_;
    array_type * values_;

    static inline int max_size(){
        return default_size;
    }

    explicit Batch(){
        size_=0;
        batch_cnt_=0;
        memory_pool_= nullptr;
        keys_= nullptr;
        values_= nullptr;
    }


    explicit Batch(BatchMemoryPool* pool_ptr):size_(0),batch_cnt_(0),memory_pool_(pool_ptr){
        allocate();
    }


    Batch(const Batch& batch) = delete;
    Batch& operator=(const Batch& batch) = delete;

    Batch(Batch&& batch){
        keys_=batch.keys_;
        values_=batch.values_;
        size_=batch.size_;
        batch.size_=0;
        batch.batch_cnt_=0;
        batch.keys_= nullptr;
        batch.values_= nullptr;
        memory_pool_=batch.memory_pool_;
    }

    Batch& operator=(Batch&& batch){
        //todo check self assign
        keys_=batch.keys_;
        values_=batch.values_;
        size_=batch.size_;
        batch.keys_= nullptr;
        batch.values_= nullptr;
        return *this;
    }

    inline bool add_tuple(const tuple_t* t){
        if(t==nullptr){
            return false;
        }
        (*keys_)[size_]=t->key;
        (*values_)[size_]=t->payloadID;
        size_++;
        return size_==default_size;
    }

    inline bool add_tuple(const tuple_t& t){
        (*keys_)[size_]=t.key;
        (*values_)[size_]=t.payloadID;
        size_++;
        return default_size==size_;
    }

    inline bool add_tuple(key_t key, value_t value){
        (*keys_)[size_]=key;
        (*values_)[size_]=value;
        size_++;
        return default_size==size_;
    }

    // called after moved
    inline void allocate(){
        size_=0;
        keys_=memory_pool_->allocate();
        values_=memory_pool_->allocate();
    }

    inline void reset(){
        size_=0;
    }

    inline int size() const{
        return size_;
    }
    inline int batch_cnt() const{
        return batch_cnt_;
    }
    ~Batch(){
        memory_pool_->deallocate(keys_);
        memory_pool_->deallocate(values_);
    }

};





