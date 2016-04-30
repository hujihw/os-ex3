// Search.cpp

#include "Search.h"
#include <string.h>
#include <sys/stat.h>


//////////////////////////
// Search Class Methods //
//////////////////////////

void Search::Map(const k1Base *const key, const v1Base *const val) const {
    // downcast base types to user types
    FileNameKey *fileNameKey = (FileNameKey*) &key;
    // todo check if the file is a dir
    // todo use fields from user types to convert to 2base types

    SubStrLocation subStrLocation;
    // Emit2 the result
    Emit2(&subStrLocation, nullptr);
}


///////////////////////////////
// FileNameKey Class Methods //
//////////////////////////////

bool FileNameKey::operator<(const k1Base &other) const {
    return this->name.compare(((FileNameKey*) &other)->name) < 0;
}


//////////////////////////////////
// SubStrLocation Class Methods //
//////////////////////////////////

bool SubStrLocation::operator<(const k2Base &other) const {
    return this->location < ((SubStrLocation*) &other)->location;
}
