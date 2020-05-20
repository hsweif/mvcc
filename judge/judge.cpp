#include "judge.h"
#include <fstream>
#include <cstring>
#include <stdint.h>
#include <algorithm>

using std::ifstream;
using std::ofstream;

vector<trans *> transVector;
map<string, int> data;
map<int, map<string, int>*> hangBuffer;

struct CSVLine{
    int tindex = -1;
    string key;
    uint64_t time;
    int tid;
};

bool operator<(const CSVLine &a, const CSVLine &b){
    return a.time > b.time;
}

void display(const CSVLine & line){
    printf("Line:%d %s %lu\n", line.tindex, line.key.c_str() , line.time);
}

vector<string> split(const string& str, const string& delim) {
	vector<string> res;
	if("" == str) return res;
	//先将要切割的字符串从string类型转换为char*类型
	char * strs = new char[str.length() + 1] ; //不要忘了
	strcpy(strs, str.c_str()); 
 
	char * d = new char[delim.length() + 1];
	strcpy(d, delim.c_str());
 
	char *p = strtok(strs, d);
	while(p) {
		string s = p; //分割得到的字符串转换为string类型
		res.push_back(s); //存入结果数组
		p = strtok(NULL, d);
	}
 
	return res;
}

CSVLine nextLine(ifstream &fin){
    string line;
    fin >> line;
    // Empty Line
    if(line.find(',') == line.npos) return CSVLine();
    vector<string> splits = split(line, ",");
    // for(auto i = 0 ; i < 4 ; ++i)
    //    printf("%d:%s,%d\n",i,splits[i].c_str(),splits[i].size());
    if((splits[1] != "BEGIN") && (splits[1] != "END")) return CSVLine();
    CSVLine csvline;
    csvline.tindex = std::stoi(splits[0]);
    csvline.key = splits[1];
    csvline.time = std::stoull(splits[2]);
    return csvline;
}

trans * parse_trans(ifstream &in, int tid = 0){
    string content;
    int value;
    int order = 0;
    in >> content;
    if(content != "BEGIN") return nullptr;
    in >> value;
    trans *t = new trans();
    t->index = value;
    while(1){
        content.clear();
        in >> content;
        if (content == "READ") {
            string src;
            in >> src;
            t->addOper(oper(src, order));
            // t->addOper(OP::r, key, 0);
        } else if (content == "SET"){
            string src, dst;
            in >> dst;
            dst = dst.substr(0, dst.size() - 1);
            in >> src;
            in >> content;
            char pon = content[0];
            in >> value;
            if(pon == '-') value = - value; 
            t->addOper(oper(dst, src, value, order));            
        } else if(content == "COMMIT"){
            in >> value;
            if(value == t->index){
                t->tid = tid;
                return t;
            }
            else {
                delete t;
                return nullptr;
            }
        } else {
            delete t;
            return nullptr;
        }
        order = order + 1;
    }
}

void loadTrans(const int &threads){
    ifstream fin[threads];
    for(auto i = 0 ; i < threads ; ++i){
        string inFile = "thread_" + std::to_string(i + 1) + ".txt";
        fin[i] = ifstream(inFile);
    }
    for(auto i = 0 ; i < threads ; ++i){
        trans *t;
        while((t = parse_trans(fin[i], i + 1)) != nullptr){
            transVector.push_back(t);
            // printf("TIndex:%d %d\n", t->index, t->opList.size());
        }
    }
}

void CommitTrans(trans *t){
    printf("Commit %d\n", t->index);
    // Commit a trans
    int index = t->index;
    auto it = hangBuffer.find(index);
    auto buffer = it->second;
    for(auto itb = buffer->begin() ; itb != buffer->end() ; ++itb) {
        auto key = itb->first;
        auto value = itb->second;
        data.find(key)->second = value;
        if(index == 16)
            printf("%s %d \n", key.c_str(), value);
    }
    delete buffer;
    hangBuffer.erase(index);
}

int findBuffer(const map<string, int> &buffer, const string &key){
    if (buffer.find(key) != buffer.end()){
        return buffer.find(key)->second;
    } else {
        return data.find(key)->second;
    }
}

map<string, int> * RunTrans(ofstream& fout, trans *t){
    // Run a trans
    printf("Run %d\n", t->index);
    map<string, int> *buffer = new map<string, int>();
    for(auto it = t->opList.begin() ; it != t->opList.end() ; ++it){
        if(it->op == OP::r){
            int value = findBuffer(*buffer, it->src);
            fout << t->index << "," << it->src << ",," <<value << "\n";
        } else {
            int value = findBuffer(*buffer, it->src);
            value += it->value;
            buffer->erase(it->dst);
            buffer->insert(std::make_pair(it->dst, value));
        }
    }
    return buffer;
}

void prepareData(){
    ifstream in("./data_prepare.txt");
    if(!in) return;
    uint line = 0;
    string content, key;
    int value;
    while(!in.eof()){
        content.clear();
        in >> content;
        if(content.empty()) break;
        switch (line % 3)
        {
        case 1:
            key = content;
            break;
        case 2:
            value = std::stoi(content);
            data.insert(std::make_pair(key, value));
            break;
        
        default:
            break;
        }
        ++ line;
    }
    printf("Insert %d Items\n", line / 3);
    in.close();
}

void dealCSVLine(ofstream &fout,const CSVLine &line){
    if(line.key == "BEGIN"){
        fout << line.tindex << ",BEGIN,,\n";
        auto buffer = RunTrans(fout, transVector[line.tindex]);
        fout << line.tindex << ",END,,\n";
        hangBuffer.insert(std::make_pair(line.tindex, buffer));
    } else if(line.key == "END"){
        CommitTrans(transVector[line.tindex]);
    }
}


void initCSV(const int &threads){
    prepareData();
    ifstream fin[threads];
    ofstream fout[threads];
    for(auto i = 0 ; i < threads ; ++i){
        string inFile = "output_thread_" + std::to_string(i + 1) + ".csv";
        fin[i] = ifstream(inFile);
    }
    for(auto i = 0 ; i < threads ; ++i){
        string outFile = "real_thread_" + std::to_string(i + 1) + ".csv";
        fout[i] = ofstream(outFile);
        fout[i] << "transaction_id,type,time,value\n";
    }
    string line;
    vector<CSVLine> csvlines;
    for(auto i = 0 ; i < threads ; ++i){
        fin[i] >> line;
        CSVLine newline = nextLine(fin[i]);
        while (newline.tindex == -1) {
            if(fin[i].eof()) break;
            newline = nextLine(fin[i]);
        }
        if(newline.tindex != -1) {
            newline.tid = i;
            csvlines.push_back(newline);
            std::push_heap(csvlines.begin(), csvlines.end());
        }
    }
    while(!csvlines.empty()){
        // The earliest line
        std::pop_heap(csvlines.begin(), csvlines.end());
        CSVLine csvline = csvlines[csvlines.size() - 1];
        csvlines.pop_back();
        // Deal with the csvline
        dealCSVLine(fout[csvline.tid], csvline);
        // Add a new line
        int i = csvline.tid;
        CSVLine newline = nextLine(fin[i]);
        while (newline.tindex == -1) {
            if(fin[i].eof()) break;
            newline = nextLine(fin[i]);
        }
        
        if(newline.tindex != -1) {
            newline.tid = i;
            csvlines.push_back(newline);
            std::push_heap(csvlines.begin(), csvlines.end());
        }
    }
    for(auto i = 0 ; i < threads ; ++i){
        fin[i].close();
        fout[i].close();
    }
}