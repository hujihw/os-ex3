// Search.cpp

#include <string>
#include "MapReduceFramework.h"

#ifndef EX3_SEARCH_H
#define EX3_SEARCH_H

class Search : MapReduceBase
{

    virtual void Map(const k1Base *const key,
                     const v1Base *const val) const override;
};

class FileNameKey : public k1Base
{

    virtual bool operator<(const k1Base &other) const override;

public:
    std::string name;
};

class SubStrLocation : public k2Base
{
public:
    int location;

    virtual bool operator<(const k2Base &other) const override;
};


#endif //EX3_SEARCH_H
