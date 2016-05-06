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

    virtual void Map(const k1Base *const key,
                     const v1Base *const val) const override;

    SearchManager(std::string searchSubString);

    ~SearchManager();

    std::string searchSubstring;
};

/**
 * The class that holds the name of the directory to search in. Inherits from
 * k1Base.
 */
class DirNameK1 : public k1Base
{
public:
    DirNameK1(const std::string newDirName);

    ~DirNameK1();

    virtual bool operator<(const k1Base &other) const override;

    std::string dirName; /** the string that holds the name of the directory */
};

/**
 * The key for the 2nd type. Inherits from the k2Base
 */
class DirNameK2 : public k2Base
{
public:
    DirNameK2(std::string newFileName);

    ~DirNameK2();

    virtual bool operator<(const k2Base &other) const override;

    std::string fileName;
};

class ContainedFiles : public v2Base
{
public:
    // list of contained files
    std::list<std::string> containedFilesList;

    ContainedFiles();

    ~ContainedFiles();
};

class FileName : public k3Base
{
public:
    FileName(std::string newFileName);

    ~FileName();

    virtual bool operator<(const k3Base &other) const override;

    std::string fileName;
};


#endif //EX3_SEARCH_H
