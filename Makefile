# define compiler and flags
CC=g++
CXXFLAGS=--std=c++11 -I. -Wall -pthread

# library variables
LIBSOURCES=MapReduceFramework.cpp
LIBHEADERS=MapReduceClient.h MapReduceFramework.h
LIBOBJECTS=$(LIBSOURCES:.cpp=.o)

# executable variables
EXECSOURCES=Search.cpp SearchDefinitions.cpp
EXECHEADERS=Search.h
EXECOBJECTS=$(EXECSOURCES:.cpp=.o)

# target variables
TARGETLIB=libMapReduceFramework.a
EXECUTABLE=Search
TAR=ex3.tar


.PHONY: all tar valgrind clean


all: $(EXECUTABLE)

# create the executable
$(EXECUTABLE): $(TARGETLIB) $(EXECSOURCES) $(EXECHEADERS)
	$(CC) $(CXXFLAGS) -o $(EXECUTABLE) $(EXECSOURCES) $(TARGETLIB)

# create the library
$(TARGETLIB): $(LIBOBJECTS)
	ar rcs $(TARGETLIB) $(LIBOBJECTS)

# build the objects for the library
$(LIBOBJECTS): $(LIBSOURCES) $(LIBHEADERS)
	$(CC) $(CXXFLAGS) -c -o $@ $^

# compile for Valgrind debugging
valgrind: $(EXECSOURCES) $(EXECHEADERS) $(LIBSOURCES) $(LIBHEADERS)
	$(CC) $(CXXFLAGS) -g -c -o $(LIBOBJECTS) $(LIBSOURCES)
	ar rcs $(TARGETLIB) $(LIBOBJECTS)
	$(CC) $(CXXFLAGS) -g -o vSearch $(EXECSOURCES) $(TARGETLIB)

# create a .tar file for submission
tar: $(EXECSOURCES) $(EXECHEADERS) $(LIBSOURCES) Makefile README
	tar cvf $(TAR) $^

# clean all files created by this Makefile
clean:
	rm *.o *.a *.tar $(EXECUTABLE)

## the commands for valgrind ##
# g++ --std=c++11 -I. -Wall -pthread -c -g -o MapReduceFramework.o MapReduceFramework.cpp
# ar rcs libMapReduceFramework.a MapReduceFramework.o
# g++ --std=c++11 -I. -Wall -pthread -g -o Search Search.cpp SearchDefinitions.cpp libMapReduceFramework.a