#include "trans.h"
#include <fstream>

oper::oper(const string &d, const string &s, const int &delta, const int &order){
    this->op = OP::s;
    this->dst = d;
    this->src = s;
    this->value = delta;
    this->order = order;
}

oper::oper(const string &s, const int &order){
    this->op = OP::r;
    this->src = s;
    this->order = order;
}

void trans::addOper(const oper &o){
    opList.push_back(o);
}