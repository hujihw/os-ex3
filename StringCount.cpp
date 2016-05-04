// StringCount.cpp

#include "StringCount.h"

int main()
{
    IN_ITEMS_LIST inLst;
    StringCount strCount;
    int threadsNum = 5;

    runMapReduceFramework(strCount, inLst, threadsNum);
}

//////////////////////////////////////
// StringCount Class Implementation //
//////////////////////////////////////

void StringCount::Map(const k1Base *const key, const v1Base *const val) const {

    StringList *strLst = (StringList*) &val;

    // for each word in the list
    for (auto str = strLst->lst.begin(); str != strLst->lst.end(); ++str)
    {
        Word *word = new Word(*str); // todo delete the instance at the end
        AppearanceList *al = new AppearanceList(); // todo delete the instance at the end

        // emmit2 the word as a k2 and a list of 1s in length 1 as the value
        Emit2(word, al);
    }
}

// --------------------------- Type 1 ------------------------------------------

///////////////////////////////////
// ListName Class Implementation //
///////////////////////////////////

bool ListName::operator<(const k1Base &other) const {
    return ((ListName*) &this)->name < ((ListName*) &other)->name;
}

// --------------------------- Type 2 ------------------------------------------

///////////////////////////////
// Word Class Implementation //
///////////////////////////////

Word::Word(std::string theWord) {
    this->str = theWord;
}

bool Word::operator<(const k2Base &other) const {
    return this->str < ((Word*) &other)->str;
}


/////////////////////////////////////////
// AppearanceList Class Implementation //
/////////////////////////////////////////

AppearanceList::AppearanceList()
{
    appearances.push_back(1);
}

// --------------------------- Type 3 ------------------------------------------

/////////////////////////////////////////
// AppearanceList Class Implementation //
/////////////////////////////////////////