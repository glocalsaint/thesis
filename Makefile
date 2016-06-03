CC=mpic++
CFLAGS=-std=c++11 -lboost_system -lboost_filesystem 

all: clean mpi.o  
	$(CC) mpi.o  $(CFLAGS) -o a.out
mpi.o:
	$(CC) -c mpi.cpp $(CFLAGS) -o mpi.o
graph.o:
	g++ -c graph.cpp $(CFLAGS) -o graph.o  
run: all
	mpirun -np 4 a.out      
clean:
	rm -f mpi.o graph.o a.out
