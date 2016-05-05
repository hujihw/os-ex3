// ExecMap.h

#ifndef EX3_EXECMAP_H
#define EX3_EXECMAP_H

#include "MapReduceFramework.h"
#include "Manager.h"
#include <vector>

class MapManager : public Manager
{
public:
    /**
     * @brief A singleton get-instance function
     *
     * @return &MapManager a reference to the object
     */
    static MapManager& getInstance(int multiThreadLevel);

    /**
     * @brief todo
     */
    MapManager(MapManager const&) = delete;

    /**
     * @brief todo
     */
    void operator=(MapManager const&) = delete;

    void RunMappingPhase(void *(start_routine) (void *));

private:
    /**
     * @brief A constructor
     */
    MapManager(int multiThreadLevel);

    /**
     * @brief This function executes several times in every thread the Map function
     *        provided by the user.
     * todo assuming number of iterations is calculated in framework start function
     * todo fix documentation
     */
    void ExecMap(void *(start_routine) (void *));


};



#endif //EX3_EXECMAP_H
