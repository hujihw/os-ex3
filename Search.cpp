#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "string.h"
#include <iostream>
#include <dirent.h>

using namespace std;

#define SUBSTRING 1
#define FIRST_DIR 2

/******************************************************************************/
/*                       Keys and Values Decleration                          */
/******************************************************************************/

class substringToSearch : public k1Base {
public:
    std::string substring;
    substringToSearch(string givenSubstring)
    {
        substring = givenSubstring;
    };
    bool operator<(const k1Base &other) const
    {
        (void)other;
        return true;
    };
};


class fileNameV1 : public v1Base {
public:
    std::string fileName;
    fileNameV1(string givenfileNameA)
    {
        fileName = givenfileNameA;
    };
};


class occurrenceK2 : public k2Base {
public:
    bool subExists;
    occurrenceK2(bool givenSubExists)
    {
        subExists = givenSubExists;
    };

    bool operator<(const k2Base &other) const
    {
        occurrenceK2 * temp = (occurrenceK2 *) &other;
        occurrenceK2 * temp2 = (occurrenceK2 *) this;

        return (temp2->subExists <  temp->subExists);
    }
};


class fileNameV2 : public v2Base {
public:
    std::string fileName;
    k2Base * keyPtr;
    fileNameV2(string givenfileNameB, k2Base *  key)
    {
        fileName = givenfileNameB;
        keyPtr = key;
    };

    ~fileNameV2(){
        delete(keyPtr);
    }
};


class occurrenceK3 : public k3Base {
public:
    bool subExists;
    occurrenceK3(bool givenSubExists)
    {
        subExists = givenSubExists;
    };

    bool operator<(const k3Base &other) const
    {
        occurrenceK3 * temp = (occurrenceK3 *) & other;
        occurrenceK3 * temp2 = (occurrenceK3 *) this;
        return (temp->subExists < temp2->subExists);
    }
};


class fileNameList : public v3Base {
public:
    V2_LIST fileNamesLst;
    fileNameList(const V2_LIST* givenfileNames)
    {
        fileNamesLst = *givenfileNames;
    };

    ~fileNameList(){
        for(auto v2Itm : fileNamesLst){
            delete(v2Itm);
        }
    };
};


class findFiles : public MapReduceBase {

    void Map(const k1Base *const key, const v1Base *const val) const{

        string substring = ((substringToSearch *) key) ->substring;
        string filename = ((fileNameV1 *) val) -> fileName;

        bool isSub = false;

        if (filename.find(substring) != string::npos){
            isSub = true;
        }
        occurrenceK2 * occurance = new occurrenceK2(isSub);
        fileNameV2 * newFilename = new fileNameV2(filename, occurance);

        Emit2(occurance, newFilename);
    };


    void Reduce(const k2Base *const key, const V2_LIST &vals) const{

        bool keyVal = ((occurrenceK2 *)key)->subExists;

        occurrenceK3 * occurance = new occurrenceK3(keyVal);
        fileNameList * newFilenameList = new fileNameList(&vals);

        Emit3(occurance, newFilenameList);
    };
};



int main(int argc,char *argv[]){

    string dirName;
    string file;
    IN_ITEMS_LIST inItems;

    DIR *dp;
    struct dirent *dirp;

    // Creating the input list
    substringToSearch * subStringK1 = new substringToSearch(argv[SUBSTRING]);

    for (int i = FIRST_DIR; i < argc; i++) {
        dirName = argv[i];

        if ((dp = opendir(dirName.c_str())) == NULL) {
            // TODO
        }

        while ((dirp = readdir(dp)) != NULL) {

            file = string(dirp->d_name);

            if ((strcmp(file.c_str(), ".") != 0) &&
                (strcmp(file.c_str(), "..") != 0)) {
                fileNameV1 *fileName = new fileNameV1(file);
                inItems.push_back(make_pair(subStringK1, fileName));
            }

        }
        closedir(dp);
    }

    OUT_ITEMS_LIST result;
    findFiles ff;

    result = runMapReduceFramework(ff, inItems, 1);

    for(auto resPair : result){
        if(((occurrenceK2 *)resPair.first)->subExists){

            V2_LIST resList = ((fileNameList *) resPair.second)->fileNamesLst;

            for(auto resItem : resList){
                string toPrint = ((fileNameV2 *) resItem)->fileName;
                cout << toPrint << endl;
            }
        }
    }


    for(auto inPair : inItems){
        delete(inPair.second);
    }
    delete(subStringK1);

    for(auto outPair : result){
        delete(outPair.first);
        delete(outPair.second);
    }
}




