CC = g++
CFLAGS += -Wall -std=c++11
DEPS = $(wildcard *.cpp)
OBJS = $(patsubst %.cpp,%.o,$(DEPS))

all: main clean

%.o:%.cpp
	$(CC) $(CFLAGS) -c $<

main: $(OBJS)
	$(CC) $(CFLAGS) $^ -o $@

clean:
	rm *.o

clear:
	rm *.csv
	rm *.txt
