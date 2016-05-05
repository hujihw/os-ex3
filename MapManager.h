// ExecMap.h

#ifndef EX3_EXECMAP_H
#define EX3_EXECMAP_H

#include "MapReduceFramework.h"
#include <vector>

class MapManager
{
public:
    /**
     * @brief A singleton get-instance function
     *
     * @return &MapManager a reference to the object
     */
    static MapManager& getInstance();

    /**
     * @brief todo
     */
    MapManager(MapManager const&) = delete;

    /**
     * @brief todo
     */
    void operator=(MapManager const&) = delete;

    /**
     * @brief This function executes several times in every thread the Map function
     *        provided by the user.
     * todo assuming number of iterations is calculated in framework start function
     * todo fix documentation
     */
    void ExecMap(void *(start_routine) (void *));

private:
    /**
     * @brief A default constructor
     */
    MapManager();

    /**
     * the data structure that holds the type 2 key-value pairs emitted by Map
     */
    std::vector<k2Base*, v2Base*> _type2Vector;
};



#endif //EX3_EXECMAP_H
