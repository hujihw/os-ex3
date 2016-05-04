// ExecMap.h

#ifndef EX3_EXECMAP_H
#define EX3_EXECMAP_H

#include "MapReduceFramework.h"
#include <vector>

class ExecMap
{
public:
    /**
     * @brief A singleton get-instance function
     *
     * @return &ExecMap a reference to the object
     */
    static ExecMap& getInstance();

    /**
     * @brief todo
     */
    ExecMap(ExecMap const&) = delete;

    /**
     * @brief todo
     */
    void operator=(ExecMap const&) = delete;

    /**
     * @brief This function executes several times in every thread the Map function
     *        provided by the user.
     * todo assuming number of iterations is calculated in framework start function
     * todo fix documentation
     */
    void MapFunctionExec(MapReduceBase &mapReduce, unsigned int iterations);

private:
    /**
     * @brief A default constructor
     */
    ExecMap();

    /**
     * the data structure that holds the type 2 key-value pairs emitted by Map
     */
    std::vector<k2Base*, v2Base*> _type2Vector;

    void MapFunctionExec(MapReduceBase &mapReduce, unsigned int iterations,
                         IN_ITEMS_LIST &itemsList);
};



#endif //EX3_EXECMAP_H
