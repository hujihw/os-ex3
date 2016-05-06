// SearchDefinitions.cpp

#include "Search.h"


//////////////////////////
// Search Class Methods //
//////////////////////////

void SearchManager::Map(const k1Base *const key,
                        const v1Base *const val) const {
    int len;
    struct dirent *pDirent;
    DIR *pDir;

    pDir = opendir(((DirNameKey*) &key)->dirName.c_str());
    if (pDir != NULL)
    {
        // for each file in the dir check if it contains the word
        while ((pDirent = readdir(pDir)) != nullptr)
        {
            // add containing files with Emit2
            std::string dirName(pDirent->d_name);
            if (dirName.find(searchSubstring))
            {
                ContainsKey *newKey = new ContainsKey(dirName);
                Emit2(newKey, nullptr);
            }
        }
    }
    closedir(pDir);
}


void SearchManager::Reduce(const k2Base *const key, const V2_LIST &vals) const
{
    FileNameKey *fileNameKey = new FileNameKey(((ContainsKey*) &key)->fileName);
    Emit3(fileNameKey, nullptr);
}


SearchManager::SearchManager(
        std::string searchSubString) : searchSubstring(searchSubString) { }

SearchManager::~SearchManager()
{

}


//////////////////////////////
// DirNameKey Class Methods //
//////////////////////////////


DirNameKey::DirNameKey(char *newDirName) : dirName(newDirName) { }


bool DirNameKey::operator<(const k1Base &other) const {
    return this->dirName < ((DirNameKey*) &other)->dirName;
}


///////////////////////////////
// ContainsKey Class Methods //
///////////////////////////////


bool ContainsKey::operator<(const k2Base &other) const {
    return this->fileName < ((ContainsKey*) &other)->fileName;
}

ContainsKey::ContainsKey(std::string &newFileName) : fileName(newFileName){ }


///////////////////////////////////////
// FileNameKey Class Implementations //
///////////////////////////////////////

bool FileNameKey::operator<(const k3Base &other) const {
    return false;
}

FileNameKey::FileNameKey(std::string &newFileName) : fileName(newFileName) { }
