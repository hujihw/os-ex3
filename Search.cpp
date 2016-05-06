// Search.cpp

#include <vector>
#include "Search.h"
#include "MapReduceFramework.h"

int main(int argc, char *argv[])
{
    // the input items vector
    IN_ITEMS_LIST inputItems;

    // print Search program usage if not enough arguments given
    if (argc < 2)
    {
        std::cout << "Usage: <substring to search> "
                     "<folders, separated by space>" << std::endl;
        exit(1);
    }

    // store the substring to search
    std::string searchString(argv[1]);
    std::cout << "search this: " << searchString << std::endl;

    // insert all input into type1 pairs
    for (int i = 2; i < argc; ++i)
    {
        std::cout << "argv[" << i << "] = " << argv[i] << std::endl; // todo remove
        DirNameKey *dirNameKey = new DirNameKey(argv[i]);
        inputItems.push_back(std::make_pair((k1Base *) dirNameKey, (v1Base*) nullptr));
        for (auto item = inputItems.begin(); item != inputItems.end(); ++item)
        {
            std::cout << " + "  << ((DirNameKey*) item.operator*().first)->dirName << std::endl;
        }
    }

    // create the SearchManager instance
    SearchManager searchManager(searchString);

    // declare the output data structure
    OUT_ITEMS_LIST outItemsList = runMapReduceFramework(searchManager, inputItems, 5);

    // print the returned values
    for (auto item = outItemsList.begin(); item != outItemsList.end(); ++item)
    {
        std::cout << item.operator*().first << std::endl;
    }

    return 0;
}
