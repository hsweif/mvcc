#include <stdio.h>
#include <fstream>
#include "judge.h"

using std::ifstream;
using std::ofstream;

int main(int argc, const char *argv[]){
    int threads = 0;
    if(argc >= 2)
        sscanf(argv[1], "%d", &threads);
    else
        threads = 1;
    printf("Threads: %d\n", threads);
    loadTrans(threads);
    initCSV(threads);
}