// ExecMap.h

#ifndef EX3_EXECMAP_H
#define EX3_EXECMAP_H

#include "MapReduceFramework.h"


/**
 * MapFunctionExec
 *
 * This function executes several times in every thread the Map function
 * provided by the user.
 * todo assuming number of iterations is calculated in framework start function
 * todo fix documentation
 */
void MapFunctionExec(MapReduceBase &mapReduce, unsigned int iterations);

#endif //EX3_EXECMAP_H
