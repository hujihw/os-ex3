// main.cpp
#include <iostream>
#include "MapReduceFramework.h"

using namespace std;

class MapReduceChild : public MapReduceBase
{
public:
    virtual void Map(const k1Base *const key,
                     const v1Base *const val) const override {
        cout << "Map Function" << endl;
    }

    virtual void Reduce(const k2Base *const key,
                        const V2_LIST &vals) const override {
        cout << "Reduce Function" << endl;
    }
};

class k1Child : public k1Base
{
public:
    virtual bool operator<(const k1Base &other) const override {
        cout << "k1 operator<" << endl;
        return false;
    }
};

class v1Child : public v1Base {};

class k2Child : public k2Base
{
public:
    virtual bool operator<(const k2Base &other) const override {
        cout << "k2 operator<" << endl;
        return false;
    }
};

class v2Child : public v2Base{};

int main()
{
    // test mapReduce object
    MapReduceChild mapReduceBase;

    // test input set
    IN_ITEMS_LIST inItemsList;

    cout << " === MapReduce Framework Test ===" << endl;

    OUT_ITEMS_LIST outItemsList = runMapReduceFramework(mapReduceBase,
                                                        inItemsList,
                                                        5);

    return 0;
}