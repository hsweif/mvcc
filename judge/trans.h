#include <string>
#include <vector>

using std::string;
using std::vector;

enum OP {e, s, r, u};

struct oper {
    OP op;
    string dst;
    string src;
    int value;
    int order;
    // For set
    oper(const string &d, const string &s, const int &delta, const int &order);
    // For read
    oper(const string &s, const int &order);
};

struct trans
{
    /* data */
    // thread*
    int index;
    int count;
    int tid;
    vector<oper> opList;
    void addOper(const oper &o);
};
