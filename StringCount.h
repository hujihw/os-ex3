// StringCount.h

#include <list>
#include <vector>
#include <string>
#include "MapReduceFramework.h"

#ifndef EX3_STRINGCOUNT_H
#define EX3_STRINGCOUNT_H

/**
 * The class for testing the Framework with lists of strings
 */
class StringCount : public MapReduceBase {

public:
    virtual void Map(const k1Base *const key,
                     const v1Base *const val) const override;
};

// k1: string - the name of the list, v1: list of stings //
class ListName : public k1Base{
public:
    virtual bool operator<(const k1Base &other) const override;

    std::string name;

};

class StringList : public v1Base {
public:
    std::list<std::string> lst;
};

// k2: a word in the k1 list, v2: a list of '1'
class Word : public k2Base {

public:
    /**
     * The string that hold the actual word
     */
    std::string str;

    /**
     * @brief A constructor with the actual word parameter
     *
     * @param string theWord the actual word to be stored ini the instance
     */
    Word(std::string theWord);

    /**
     * @brief A "less than" operator
     *
     * @return bool true if "this" is less than "other"
     */
    virtual bool operator<(const k2Base &other) const override;
};

class AppearanceList : public v2Base {
public:
    AppearanceList();

    std::vector<int> appearances;
};

// k3: the word from k2, v3: the sum of the list given in v2
class word : public k3Base {

};

class TimesAppears : public v3Base {

};

#endif //EX3_STRINGCOUNT_H
