// Manager.h

#include <vector>
#include <sys/types.h>
#include "MapReduceFramework.h"

#ifndef EX3_MANAGER_H
#define EX3_MANAGER_H


virtual class Manager { // todo keep virtual?
public:
    std::vector<pthread_t> threads;

};


#endif //EX3_MANAGER_H
