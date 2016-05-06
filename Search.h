// Search.cpp

#include <string>
#include <dirent.h>
#include <iostream>
#include "MapReduceFramework.h"

#ifndef EX3_SEARCH_H
#define EX3_SEARCH_H

/**
 * The class that implements the Map and Reduce functions
 */
class SearchManager : public MapReduceBase
{
public:
    virtual void Reduce(const k2Base *const key,
                        const V2_LIST &vals) const override;

    ~SearchManager();

    SearchManager(std::string searchSubString);

    virtual void Map(const k1Base *const key,
                     const v1Base *const val) const override;



    std::string searchSubstring;
};

/**
 * The class that holds the name of the directory to search in. Inherits from
 * k1Base.
 */
class DirNameKey : public k1Base
{
public:
    DirNameKey(char *newDirName);

    virtual bool operator<(const k1Base &other) const override;

    std::string dirName; /** the string that holds the name of the directory */
};

/**
 * the value for the DirName type (search destination input).
 * Suppose to be null. Inherits from v1Base.
 */
class DirNameValue : public v1Base { };

/**
 * The key for the 2nd type. Inherits from the k2Base
 */
class ContainsKey: public k2Base
{
public:
    ContainsKey(std::string &newFileName);

    virtual bool operator<(const k2Base &other) const override;

    std::string fileName;
};

class ContainsValue : public v2Base {};

class FileNameKey : public k3Base
{

public:
    virtual bool operator<(const k3Base &other) const override;
};

class FileNameValue : public v3Base {};

#endif //EX3_SEARCH_H
