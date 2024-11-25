#pragma once

// allocate len: how many T slots to allocate
template<class T, size_t AllocLen,size_t ArrayLen=1>
class MemoryPool{
private:
    std::list<T*> memory;
    T* next_pos;
    T* end_pos;

public:
    explicit MemoryPool(){
        T* mem=new T[AllocLen];
        memory.push_back(mem);
        next_pos=mem;
        end_pos=mem+AllocLen;
    }

    MemoryPool(const MemoryPool& pool)=delete;
    MemoryPool(MemoryPool&& pool){
        memory=std::move(pool.memory);
        next_pos=pool.next_pos;
        end_pos=pool.end_pos;
    }

    MemoryPool& operator=(const MemoryPool& pool)=delete;

    MemoryPool& operator=(MemoryPool&& pool)=delete;


    T* allocate(){
        if(next_pos>=end_pos){
            T* mem=new T[AllocLen];
            memory.push_back(mem);
            next_pos=mem+ArrayLen;
            end_pos=mem+AllocLen;
            return mem;
        }
        T* result=next_pos;
        next_pos=next_pos+ArrayLen;
        return result;
    }

    ~MemoryPool(){
        for(auto i=memory.begin();i!=memory.end();i++){
            delete[] *i;
        }
    };


};