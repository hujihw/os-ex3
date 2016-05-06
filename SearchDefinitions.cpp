// SearchDefinitions.cpp

#include "Search.h"


//////////////////////////
// Search Class Methods //
//////////////////////////

void SearchManager::Map(const k1Base *const key,
                        const v1Base *const val) const {
    struct dirent *pDirent;
    DIR *pDir;

    const char* dirPath = (((const DirNameK1 *const) key)->dirName).c_str();

    ContainedFiles *containedFiles = new ContainedFiles();

    pDir = opendir(dirPath);

    if (pDir != NULL)
    {
        // for each file in the dir check if it contains the word
        while ((pDirent = readdir(pDir)) != nullptr)
        {
            std::string fileName(pDirent->d_name);

            containedFiles->containedFilesList.push_back(fileName);

        }

        DirNameK2 *dirName =
                new DirNameK2(((const DirNameK1 *const) key)->dirName);

        Emit2(dirName, containedFiles);
    }
    closedir(pDir);
}


void SearchManager::Reduce(const k2Base *const key, const V2_LIST &vals) const
{
    for (auto filesList = vals.begin(); filesList != vals.end(); ++filesList) {

        ContainedFiles *containedFiles = (ContainedFiles*)(*filesList);

        for (auto currentFileName = containedFiles->containedFilesList.begin(); currentFileName != containedFiles->containedFilesList.end(); ++currentFileName)
        {
            if (std::string(*currentFileName).find(searchSubstring) != std::string::npos)
            {
                FileName *newFileName = new FileName((*currentFileName));
                Emit3(newFileName, nullptr);
            }
        }
    }
}


SearchManager::SearchManager(
        std::string searchSubString) : searchSubstring(searchSubString) { }

SearchManager::~SearchManager()
{

}


//////////////////////////////
// DirNameK1 Class Methods //
//////////////////////////////


DirNameK1::DirNameK1(const std::string newDirName) : dirName(newDirName) {}

DirNameK1::~DirNameK1() {}

bool DirNameK1::operator<(const k1Base &other) const {
    return this->dirName < ((DirNameK1 *) &other)->dirName;
}


///////////////////////////////
// DirNameK2 Class Methods //
///////////////////////////////


bool DirNameK2::operator<(const k2Base &other) const {
    return this->fileName < ((DirNameK2 *) &other)->fileName;
}

DirNameK2::DirNameK2(std::string newFileName) : fileName(newFileName){ }

DirNameK2::~DirNameK2() { }


/////////////////////////////////////////
// ContainedFiles Class Implementation //
/////////////////////////////////////////


ContainedFiles::ContainedFiles() {

}


ContainedFiles::~ContainedFiles() {

}


///////////////////////////////////////
// FileName Class Implementations //
///////////////////////////////////////


bool FileName::operator<(const k3Base &other) const {
    return false;
}

FileName::FileName(std::string newFileName) : fileName(newFileName) { }

FileName::~FileName() { }
