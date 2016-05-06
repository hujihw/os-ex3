#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "string.h"
#include <iostream>
#include <dirent.h>

using namespace std;

#define SUBSTRING 1
#define FIRST_DIR 2
#define SEPARATOR " "
#define IGNORE_FILE_1 "."
#define IGNORE_FILE_2 ".."
#define NO_ARGS 1
#define MISS_USAGE_MSG "Usage: <substring to search> <folders, separated by space>"

/******************************************************************************/
/*                       Keys and Values Declaration                          */
/******************************************************************************/


class substringToSearch : public k1Base {
public:

    // The input substring
    string substring;

    substringToSearch(string givenSubstring) {
        substring = givenSubstring;
    }

    // Empty operator
    bool operator<(const k1Base &other) const {
        (void)other;
        return true;
    }
};


class fileNameV1 : public v1Base {
public:

    // File name from one of the input directories
    string fileName;

    fileNameV1(string newFileName) {
        fileName = newFileName;
    }
};


class occurrenceK2 : public k2Base {
public:

    // Indicating if the substring was found in the file names
    bool subExists;

    occurrenceK2(bool subExistsFlag) {
        subExists = subExistsFlag;
    }

    bool operator<(const k2Base &other) const {

        // Down casting and using the bool operator
        occurrenceK2 * temp = (occurrenceK2 *) &other;
        occurrenceK2 * temp2 = (occurrenceK2 *) this;

        return (temp2->subExists <  temp->subExists);
    }
};


class fileNameV2 : public v2Base {
public:

    // File name, kept after Map
    string fileName;

    // Key pointed for memory control
    k2Base * keyPtr;

    fileNameV2(string newFileName, k2Base * key) {
        fileName = newFileName;
        keyPtr = key;
    }

    ~fileNameV2(){
        delete(keyPtr);
    }
};


class occurrenceK3 : public k3Base {
public:

    // Indicating if the substring was found in the file names
    bool subExists;

    occurrenceK3(bool subExistsFlag) {
        subExists = subExistsFlag;
    }

    bool operator<(const k3Base &other) const {

        // Down casting and using the bool operator
        occurrenceK3 * temp = (occurrenceK3 *) & other;
        occurrenceK3 * temp2 = (occurrenceK3 *) this;

        return (temp->subExists < temp2->subExists);
    }
};


class fileNameList : public v3Base {
public:

    // File names that exists or don't
    V2_LIST fileNamesList;

    fileNameList(const V2_LIST* givenNames) {
        fileNamesList = *givenNames;
    }

    ~fileNameList(){

        // Deleting all V2 in the list
        for(auto v2Itm : fileNamesList){
            delete(v2Itm);
        }
    }
};


/******************************************************************************/
/*                     Search Map and Reduce Functions                        */
/******************************************************************************/

class SearchFunctions : public MapReduceBase {

    void Map(const k1Base *const key, const v1Base *const val) const{



        // Down casting the key and value
        string substring = ((substringToSearch *) key) ->substring;
        string filename = ((fileNameV1 *) val) -> fileName;

//        std::cout<<substring<<std::endl; // todo remove
//        std::cout<<filename<<std::endl; // todo remove


        // Checking if the string is a substring of the file name
        bool isSub = false;
        if (filename.find(substring) != string::npos){
            isSub = true;
        }

        // Creating the appropriate K2 V2, and calling Emit 2
        occurrenceK2 * occurrence = new occurrenceK2(isSub);
        fileNameV2 * newFilename = new fileNameV2(filename, occurrence);

        Emit2(occurrence, newFilename);
    };


    void Reduce(const k2Base *const key, const V2_LIST &vals) const{

        // Down casting the key and value
        bool keyVal = ((occurrenceK2 *)key)->subExists;

//        std::cout<<"key val:" <<std::endl; // todo remove


        // Creating the appropriate K3 V3, and calling Emit 3
        occurrenceK3 * occurrence = new occurrenceK3(keyVal);
        fileNameList * newFilenameList = new fileNameList(&vals);

        Emit3(occurrence, newFilenameList);
    };
};


/******************************************************************************/
/*                           Search Main Function                             */
/******************************************************************************/

int main(int argc,char *argv[]){

    // If no arguments are given
    if(argc == NO_ARGS){
        cerr << MISS_USAGE_MSG << endl;
        exit(0);
    }

    string directory;
    string file;
    DIR *dp;

    struct dirent *dirp;
    bool notEmpty = false;

    IN_ITEMS_LIST inputItems;


    // Creating the input list
    substringToSearch * subStringK1 = new substringToSearch(argv[SUBSTRING]);

    for (int i = FIRST_DIR; i < argc; i++) {

        directory = argv[i];
        if ((dp = opendir(directory.c_str())) == NULL) {
            continue;
        }

        // Reading files from the directory
        while ((dirp = readdir(dp)) != NULL) {

            file = string(dirp->d_name);

            if ((strcmp(file.c_str(), IGNORE_FILE_1) != 0) &&
                (strcmp(file.c_str(), IGNORE_FILE_2) != 0)) {

                fileNameV1 *fileName = new fileNameV1(file);
                inputItems.push_back(make_pair(subStringK1, fileName));
            }

        }
        closedir(dp);
    }

    OUT_ITEMS_LIST result;
    SearchFunctions searchFunctions;

    result = runMapReduceFramework(searchFunctions, inputItems, 5);


    // Printing results
    for(auto resPair : result){

        // Printing only files that include the substring
        if(((occurrenceK2 *)resPair.first)->subExists){

            V2_LIST resList = ((fileNameList *) resPair.second)->fileNamesList;

            for(auto resItem : resList){
                notEmpty = true;
                string toPrint = ((fileNameV2 *) resItem)->fileName;
                cout << toPrint <<  SEPARATOR;
            }
        }
    }

    // If any files are printed, line down
    if(notEmpty){
        cout << endl;
    }


    // Freeing memory
    for(auto inPair : inputItems){
        delete(inPair.second);
    }

    delete(subStringK1);

    for(auto outPair : result){
        delete(outPair.first);
        delete(outPair.second);
    }
}