#pragma once

#include "../utils/types.h"
#include "memory_pool.h"

class Batch{
public:
    constexpr static int default_size=256;
    using BatchMemoryPool=MemoryPool<int32_t ,256*default_size,default_size>;

private:




    // debugging field
    int batch_cnt_;

    BatchMemoryPool* memory_pool_;



public:


    int32_t* keys_;
    int32_t* values_;
    int size_;

    static inline int max_size(){
        return default_size;
    }

//    explicit Batch(){
//        size_=0;
//        batch_cnt_=0;
//        memory_pool_= nullptr;
//        keys_= nullptr;
//        values_= nullptr;
//    }


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
        memory_pool_=batch.memory_pool_;
        return *this;
    }

    inline bool add_tuple(const tuple_t* t){
        if(t==nullptr){
            return false;
        }
        keys_[size_]=t->key;
        values_[size_]=t->payloadID;
        size_++;
        return size_==default_size;
    }

    inline bool add_tuple(const tuple_t& t){
        keys_[size_]=t.key;
        values_[size_]=t.payloadID;
        size_++;
        return default_size==size_;
    }

    inline bool add_tuple(key_t key, value_t value){
        keys_[size_]=key;
        values_[size_]=value;
        size_++;
        return default_size==size_;
    }

    // called after moved
    inline void allocate(){
        size_=0;
        keys_=memory_pool_->allocate();
        values_=memory_pool_->allocate();
    }

    __always_inline
    key_t* keys() const{
        return keys_;
    }

    __always_inline
    value_t * values() const{
        return values_;
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
        // memory pool release all memory after join end
//        memory_pool_->deallocate(keys_);
//        memory_pool_->deallocate(values_);
    }

};





