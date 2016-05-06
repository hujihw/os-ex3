// ExecMap.h

#ifndef EX3_EXECMAP_H
#define EX3_EXECMAP_H

#include "MapReduceFramework.h"
#include "Manager.h"
#include <vector>
#include <iostream>

typedef std::map<k2Base*, std::list<v2Base*>> ShuffledMap;

class MapManager : public Manager
{
public:
    /**
     * @brief A singleton get-instance function
     *
     * @return &MapManager a reference to the object
     */
    static MapManager& getInstance(int multiThreadLevel, IN_ITEMS_LIST &inItemsList);

    /**
     * @brief todo
     */
    MapManager(MapManager const&) = delete;

    /**
     * @brief todo
     */
    void operator=(MapManager const&) = delete;

    ShuffledMap* RunMappingPhase(void (*start_routine)(const k1Base *const, const v1Base *const));

    ShuffledMap shuffledMap;

private:
    /**
     * @brief A constructor
     */
    MapManager(int multiThreadLevel, IN_ITEMS_LIST &inItemsList);

    /**
     * @brief This function executes several times in every thread the Map function
     *        provided by the user.
     * todo assuming number of iterations is calculated in framework start function
     * todo fix documentation
     */
    void * ExecMap(void *start_routine);

    IN_ITEMS_LIST _inItemsList;


};



#endif //EX3_EXECMAP_H
