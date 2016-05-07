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

# create a .tar file for submission
tar: $(EXECSOURCES) $(EXECHEADERS) $(LIBSOURCES) Makefile README
	tar cvf $(TAR) $^

# compile for Valgrind debugging
valgrind: $(EXECSOURCES) $(EXECHEADERS) $(LIBSOURCES) $(LIBHEADERS)
	$(CC) $(CXXFLAGS) -g -c -o $(LIBOBJECTS) $(LIBSOURCES) $(LIBHEADERS)
	ar rcs $(TARGETLIB) $(LIBOBJECTS)
	$(CC) $(CXXFLAGS) -g -o vSearch $(TARGETLIB)

# clean all files created by this Makefile
clean:
	rm *.o *.a *.tar $(EXECUTABLE)
