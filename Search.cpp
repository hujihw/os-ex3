// Search.cpp

#include "Search.h"
#include <string.h>
#include <sys/stat.h>


//////////////////////////
// Search Class Methods //
//////////////////////////

void Search::Map(const k1Base *const key, const v1Base *const val) const {
    // downcast base types to user types
    // check if the file is a dir
    // use fields from user types to convert to 2base types
    // Emit2 the result
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
