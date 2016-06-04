/*///////////Includes and defines/////////////*/
    #include <iostream> 
    #include "mpi.h"
    #include <string>
    #include <stdlib.h>
    #include <algorithm>
    #include <unordered_map>
    #include <boost/tokenizer.hpp>
    #include <set>
    #include <boost/filesystem.hpp>
    #include <boost/filesystem/operations.hpp>
    #include <boost/range/iterator_range.hpp>
    #include <iostream>
    #include <fstream>
    #include <map>
    #include <ctime>
    #include <ctype.h>
    //#include "graph.hpp"
    //#define CHUNKSIZE 100000
    #define ROUNDROBINSIZE 300
    #define STRING_LENGTH 40
    #define NUM_OF_WORDS 10
    using namespace std;
/*///////////Includes and defines/////////////*/

/*///////////////Static Declarations/////////////*/
    typedef std::unordered_map<string, std::unordered_map<string, int>> map_stringtostringint;

    unsigned long mapsize(const std::unordered_map<string,std::unordered_map<string,int>> &map){
        unsigned long mapelements = sizeof(map);
        unsigned long submapelements =0;
        for(std::unordered_map<string,std::unordered_map<string,int>>::const_iterator it = map.begin(); it != map.end(); ++it){
            submapelements += (it->second).size();        
        }
        //extra storage per node (parent,left,right, color etc)
        unsigned long mapsize = (mapelements+submapelements) *20;
        mapsize+=mapelements*STRING_LENGTH;
        mapsize+=submapelements*92;
        mapsize = mapsize/(1024*1024);
        return mapsize;
    }

    struct cmp_charstr
    {
       bool operator()(char const *a, char const *b)
       {
          return std::strcmp(a, b) < 0;
       }
    };


    struct Wordtoword
    {
        char from[STRING_LENGTH];
        char to[STRING_LENGTH];
        int count;
        // Wordtoword(string from_str, string to_str, int icount){
        //     strcpy(from,from_str.c_str());
        //     strcpy(to,to_str.c_str());
        //     count = icount;
        // }

        void setvalues(string str1, string str2, int inputcount)
        {
            //from = new char[str1.length()+1];
            strcpy(from,str1.c_str());
            //to = new char[str2.length()+1];
            strcpy(to,str2.c_str());
            count=inputcount;
        }
     /*   ~Wordtoword()
        {
            if(from!=nullptr)delete(from);
            if(to!=nullptr)delete(to);
        }*/
    };

     struct WordFrequency
     {
        char word[STRING_LENGTH];
        int count;
        void setvalues(string &str1, int inputcount)
        {
            //word = new char[str1.length()+1];
            strcpy(word,str1.c_str());        
            count=inputcount;
        }
    /*    ~WordFrequency()
        {
            if(word!=nullptr)delete(word);
        }*/
     };


     std::vector<string> getallfilenames(boost::filesystem::path p)
    {
        std::vector<string> filepaths;
        boost::filesystem::directory_iterator end_ptr;      
        boost::filesystem::directory_iterator dir(p);   
        for (;dir != end_ptr; dir++) {
            p = boost::filesystem::path(*dir);
            if(is_directory(p))
            {
                getallfilenames(p);
            }
            else
            {
                    string dirpath(dir->path().parent_path().string() );
                    string filename(p.stem().string());
                    filepaths.push_back(dir->path().string());
            }
        }
        return filepaths;
    }
/*///////////////Static Declarations/////////////*/

/*///////////////Reduce Function/////////////////*/    
    void reduce(Wordtoword* words, int count, std::unordered_map<string,std::unordered_map<string,int>> &reducedmap,int second=0)
     {  
        for(int i=0;i<count;i++)
        {        
            Wordtoword &word = words[i];
            string from_str(word.from);
            string to_str(word.to);
            if(reducedmap.find(from_str)==reducedmap.end())
            {                              
                std::unordered_map<string,int> newsubmap;
                newsubmap[to_str]=word.count;
                reducedmap[from_str]= newsubmap;            
            }
            else
            {          
                std::unordered_map<string, int> &submap = reducedmap[from_str];            
                if(submap.find(to_str)==submap.end())
                {
                    submap[to_str]=word.count;
                }
                else if(!second)
                {
                    int presentcount = submap[to_str];
                    submap[to_str]= word.count + presentcount;     
                        
                }
            }
        }
     }
/*///////////////Reduce Function/////////////////*/    

/*///////////////Reduce Frequency Function/////////////////*/         
     void reducefreq(WordFrequency* wordsfreq, int count, std::unordered_map<string,int> &frequencymap, int secondone=0)
     {    
        string current_word = "";    
        for(int i=0;i<count;i++)
        {
            WordFrequency wordfreq = wordsfreq[i];
            string current_word(wordfreq.word);    
            if(frequencymap.find(current_word)!=frequencymap.end())
            {
                if(secondone==0)
                    frequencymap[current_word]+=wordfreq.count;
                else
                    if(frequencymap[current_word]<wordfreq.count)
                        frequencymap[current_word]=wordfreq.count;
            }else if(secondone==1)
            {
                frequencymap[current_word]=wordfreq.count;
            }
        }
     }
/*///////////////Reduce Frequency Function/////////////////*/         

/*///////////////GetWordToWord Function/////////////////*/         
      Wordtoword * get_wordtoword_ifpresent(const std::unordered_map<string,  std::unordered_map<string,int>> &localmap,int &numofwords, const char (*lookup_strings)[STRING_LENGTH], int lookupcount)
     {    
        numofwords=0;
        for(int index=0;index<lookupcount;index++)
        {
            string str=lookup_strings[index];
            if(localmap.find(str)!=localmap.end())
            {            
                auto it = localmap.find(str);
                numofwords = numofwords + (it->second).size();   
            }
        }

        
        Wordtoword *wordtoword_ptr= new Wordtoword[numofwords];
        int index=0;
        int k=0;
        
        for(int index=0;index<lookupcount;index++)
        {
            string str=lookup_strings[index];
            auto it = localmap.find(str);
            if(it!=localmap.end())
            {
                const std::unordered_map<string, int> &submap = it->second;
                std::unordered_map<string, int>::const_iterator itersubmap = submap.begin();
                for(;itersubmap!=submap.end();itersubmap++ ,k++)
                {
                    Wordtoword &word= wordtoword_ptr[k];
                    word.setvalues(str,itersubmap->first,itersubmap->second);
                    //strcpy(wordtoword_ptr[k].from,str.c_str());
                    //strcpy(wordtoword_ptr[k].to,(itersubmap->first).c_str());
                    //wordtoword_ptr[k].count=itersubmap->second;
                }
            }
        }
        return wordtoword_ptr;
     }
/*///////////////GetWordToWord Function/////////////////*/         


/*///////////////GetWordFrequency Function/////////////////*/         
    WordFrequency * get_wordfrequency_ifpresent(std::unordered_map<string,int> &frequencymap, int &numofwordfreqstosend, const char (*lookup_strings)[STRING_LENGTH], int lookupcount, int allwords=0)
     {    
        if(allwords==0){
        WordFrequency *wordfrequency_ptr= new WordFrequency[lookupcount];
        int index=0;    
        numofwordfreqstosend=0;
        for(index=0;index<lookupcount;index++)
        {
            string str(lookup_strings[index]);  
                 
            if(frequencymap.find(str)!=frequencymap.end())
            {            
                    strcpy(wordfrequency_ptr[numofwordfreqstosend].word,str.c_str());         
                    wordfrequency_ptr[numofwordfreqstosend].count=frequencymap[str];
                    numofwordfreqstosend++;
                    //cout<<"Came-----------"<<frequencymap.size()<<"\n"; 
            }        
        }
        return wordfrequency_ptr;}
        else{
            int fmapsize = frequencymap.size();
            WordFrequency *wordfrequency_ptr= new WordFrequency[fmapsize];
            int index=0;    
            numofwordfreqstosend=fmapsize;
             for(index=0;index<fmapsize;index++ )//for(auto &pair : frequencymap)
             {
                string str("visa");//(*iter).first);
                strcpy(wordfrequency_ptr[index].word,str.c_str());         
                wordfrequency_ptr[index].count=3;//(*iter).second;
                //index++;
            }        
            cout<<"saldkfajsflksdjf "<< index<<" s "<< numofwordfreqstosend<<endl;
            return wordfrequency_ptr;
         }

     }
/*///////////////GetWordFrequency Function/////////////////*/        

/*///////////////Remove Entries/////////////////*/        
     void removeentries(std::unordered_map<string,std::unordered_map<string,int>> &localmap, char lookup_strings[][STRING_LENGTH], int lookupcount)
     {
        for(int i=0;i<lookupcount;i++)
        {
            string str(lookup_strings[i]);
            localmap.erase(str);
        }
     }
/*///////////////Remove Entries/////////////////*/        

/*///////////////Insert to Local Map Function/////////////////*/         
     void insert_to_localmap(std::set<string> &stringset, std::unordered_map<string,std::unordered_map<string,int>> &localmap)
     {
        
        std::set<string>::iterator it;
        std::set<string>::iterator iter;
        int j=0;
        it=stringset.begin();
        int myrank = MPI::COMM_WORLD.Get_rank(); 
        //processes local map that does not have the string entry
        for(int current_index=0; it!=stringset.end(); it++,current_index++,j=0)
        {
            string itstr(*it);
            if(localmap.find(itstr)==localmap.end())
            {
                std::unordered_map<string,int> newmap;
                for(j=0,iter = stringset.begin();iter != stringset.end();++iter)
                {
                    string iterstr(*iter);
                    //add all other words in the sentence as cooccuring words except for the current word.
                    //current_index==j refers the word which we are dealing with now.
                    if(current_index==j){j++; continue;}

                    newmap[iterstr]=1;
                    j++;
                }
                localmap[itstr]=newmap;
            }
            //processes local map that has the string entry
            else
            {        
                
                std::unordered_map<string,int> &stringmap = localmap[*it];
                for(j=0,iter = stringset.begin();iter != stringset.end();++iter)
                {
                    string iterstr(*iter);
                    //skip the current word
                    if(current_index==j) {j++;continue;}
                    if(stringmap.find(iterstr)==stringmap.end())
                    {
                        stringmap[iterstr]=1;
                    }
                    else
                    {
                        int stringcount = stringmap[iterstr]+1;
                        stringmap[iterstr] = stringcount;                    
                    }
                    j++;
                }
            }
        }
     }
/*///////////////Insert to Local Map Function/////////////////*/         

/*///////////////Process String Function/////////////////*/              
     void process_string(string &str, std::unordered_map<string,std::unordered_map<string,int>> &localmap, std::unordered_map<string, int> &frequencymap)
     {
            
            int myrank = MPI::COMM_WORLD.Get_rank(); 
            int current_index=0, index=0;
            int length = str.length();
            
            std::set<string> uniquewords;
            int k=0;
            while(index<length)
            {
                index = str.find('\n',index+1);
                if(current_index+1 == index) continue;
                if(index==string::npos) break;
                string line = str.substr(current_index,index - current_index);
                boost::char_separator<char> sep("\t ");
                boost::tokenizer<boost::char_separator<char>> tokens(line, sep);
                int i=0; string two,three;
                for (const auto& t : tokens) {
                    if(i==2) {two.assign(t); if(isdigit(two.at(0)))break;}
                    if(i==3)
                    {
                        if(t.compare("NN")==0 || t.compare("ADJ")==0)
                        {
                            string uniqueword(two+"::"+t);
                            if(uniqueword.length()>STRING_LENGTH-2)break;
                            uniquewords.insert(uniqueword);
                            //cout<<uniqueword<<endl;
                            if(frequencymap.find(uniqueword)==frequencymap.end())
                            {
                                frequencymap[uniqueword]=1;
                            }
                            else
                            {
                                int stringcount = frequencymap[uniqueword]+1;
                                frequencymap[uniqueword] = stringcount;
                            }
                        }
                    }
                    i++;
                    if(i>3) break;
                }
                if((str[index+1]=='\n' ||index==str.size()-1) && !uniquewords.empty())
                {   
                    insert_to_localmap(uniquewords,localmap);  
                    uniquewords.clear();                              
                }
                current_index=index;
            }
     }
/*///////////////Process String Function/////////////////*/              
/*///////////////GetWordToWord Instant Function/////////////////*/         
      Wordtoword * get_wordtoword_instant(const map_stringtostringint &localmap, int &numofwords, vector<string> &lookupstrings, int* counts)
     {    
        numofwords=0;

        int lookupstringssize = lookupstrings.size();
        int perprocesscount=0;
        for(int i=0;i<lookupstringssize/NUM_OF_WORDS;i++)
        {
            counts[i]=0;
            for(int j=0;j<NUM_OF_WORDS;j++)
            {
                string str=lookupstrings[i*NUM_OF_WORDS+j];
                if(localmap.find(str)!=localmap.end())
                {            
                    auto it = localmap.find(str);
                    counts[i] = counts[i] + (it->second).size();   
                }
            }
            numofwords+=counts[i];
        }
               
        Wordtoword *wordtoword_ptr= new Wordtoword[numofwords];
        
        int index=0;
        int k=0;        
        for(int index=0;index<lookupstringssize;index++)
        {
            string str=lookupstrings[index];
            auto it = localmap.find(str);
            if(it!=localmap.end())
            {
                const std::unordered_map<string, int> &submap = it->second;
                std::unordered_map<string, int>::const_iterator itersubmap = submap.begin();
                for(;itersubmap!=submap.end();itersubmap++ ,k++)
                {
                    Wordtoword &word= wordtoword_ptr[k];
                    word.setvalues(str,itersubmap->first,itersubmap->second);
                }
            }
        }
        return wordtoword_ptr;
     }
/*///////////////GetWordToWord Function/////////////////*/         

/*///////////////GetWordToWord Instant second level Function/////////////////*/         
     Wordtoword* get_wordtoword_secondlevel_instant(const map_stringtostringint &localmap, vector<string> &lookupstrings, int *firstlevelcounts, int *sl_wordtoword_counts, int size)      
     {    
        int lookupstringssize = lookupstrings.size();        
        auto it = lookupstrings.begin();
        int totalwordssize=0;
        for(int i=0;i<size;i++)
        {
            sl_wordtoword_counts[i]=0;
            for(int j=0;j<firstlevelcounts[i];j++)
            {
                auto it1 = localmap.find(*it);
                if(it1!=localmap.end())
                {                    
                    sl_wordtoword_counts[i]+=(it1->second).size();                    
                }
                it++;
            }
            totalwordssize+=sl_wordtoword_counts[i];
        }   
               
        Wordtoword *wordtoword_ptr= new Wordtoword[totalwordssize];
                
        int index=0;
        int k=0;        
        for(int index=0;index<lookupstringssize;index++)
        {
            string str=lookupstrings[index];
            auto it = localmap.find(str);
            if(it!=localmap.end())
            {
                const auto &submap = it->second;
                std::unordered_map<string, int>::const_iterator itersubmap = submap.begin();
                for(;itersubmap!=submap.end();itersubmap++ ,k++)
                {
                    Wordtoword &word= wordtoword_ptr[k];
                    word.setvalues(str,itersubmap->first,itersubmap->second);
                }
            }
        }
        return wordtoword_ptr;
     }
/*///////////////GetWordToWord Instant second level Function/////////////////*/         

int main(int argc, char *argv[]) 
{ 

    MPI::Init(argc, argv); 
/*//////Datatype for Sending words*/
    MPI_Datatype MPI_Customword;
    MPI_Datatype type0[1] = { MPI_CHAR };
    int blocklen0[1] = { STRING_LENGTH};
    MPI_Aint disp0[1];
 
    disp0[0] = 0;
    
    MPI_Type_create_struct(1, blocklen0, disp0, type0, &MPI_Customword);
    MPI_Type_commit(&MPI_Customword);
/*//////Datatype for Sending words*/


/*//////Datatype for Sending word cooccurances*/
    MPI_Datatype MPI_SingleWordtoWord;
    MPI_Datatype type[3] = { MPI_CHAR, MPI_CHAR, MPI_INT };
    int blocklen[3] = { STRING_LENGTH, STRING_LENGTH , 1 };
    MPI_Aint disp[3];
 
    disp[0] = 0;
    disp[1] = STRING_LENGTH;
    disp[2] = STRING_LENGTH*2;
    MPI_Type_create_struct(3, blocklen, disp, type, &MPI_SingleWordtoWord);
    MPI_Type_commit(&MPI_SingleWordtoWord);
/*//////Datatype for Sending word cooccurances*/

/*//////Datatype for Sending word frequencies*/
    MPI_Datatype MPI_SingleWordFrequency;
    MPI_Datatype type1[2] = { MPI_CHAR, MPI_INT };
    int blocklen1[2] = { STRING_LENGTH, 1 };
    MPI_Aint disp1[2];
 
    disp1[0] = 0;
    disp1[1] = STRING_LENGTH;    
    MPI_Type_create_struct(2, blocklen1, disp1, type1, &MPI_SingleWordFrequency);
    MPI_Type_commit(&MPI_SingleWordFrequency);
/*//////Datatype for Sending word frequencies*/

/*//////Declarations//////*/
    MPI::Status status; 
    int myrank = MPI::COMM_WORLD.Get_rank(); 
    int size = MPI::COMM_WORLD.Get_size(); 
    std::vector<string> files=getallfilenames("/work/scratch/vv52zasu/inputfiles/");

    std::unordered_map<string,int> frequencymap;
    std::unordered_map<string,std::unordered_map<string, int>> localmap;
    std::unordered_map<string,std::unordered_map<string, int>> localsecondlevelmap;
    int bufsize, *buf;
    string bufchar1;
    char filename[128]; 
/*//////Declarations//////*/    

/*//////Read files in a loop and write initial data to localmap/////*/
    std::clock_t fileprocessing_timestart = clock();
    for(std::vector<string>::iterator it = files.begin(); it != files.end(); ++it)
    {
        if(myrank ==0)
            std::cout<<"Processing file:"<<(*it).c_str()<<endl;

        MPI::File thefile = MPI::File::Open(MPI::COMM_WORLD, (*it).c_str(), MPI::MODE_RDONLY, MPI::INFO_NULL); 
        MPI::Offset filesize = thefile.Get_size();
       
        char *bufchar;  
        int CHUNKSIZE = (filesize/size)+1;
        CHUNKSIZE = std::max(CHUNKSIZE, 10000);
        bufchar =  new char[CHUNKSIZE+2]; 

        MPI_Status status1;
        int i=0;

        MPI_File_seek(thefile, (myrank)*CHUNKSIZE, MPI_SEEK_SET);
        MPI_File_read( thefile, bufchar, CHUNKSIZE, MPI_CHAR, &status1);
        int count=0;
        MPI_Get_count( &status1, MPI_CHAR, &count );
        string str(bufchar,bufchar+count);
        
        MPI::COMM_WORLD.Barrier();
        
        int occurrences = 0;
        string::size_type start = 0;
        
        int from =0, to=0,index=0;
        string tosend(str);
        while(1)
        {
            index=index+to+1;
            to = tosend.find("\n\n");      
            if(to==string::npos) break;        
            tosend=tosend.substr(to+1);
        }
        string trimstr=str.substr(0,index-1);        
        
        str="";
        MPI::COMM_WORLD.Barrier();

        int length = tosend.length();
        char *recvptr;
        recvptr = new char[CHUNKSIZE];
        
        int dest=0,src=0;
        if(myrank==size-1)
        {
            dest=0;
            src=myrank-1;
        }
        else if(myrank==0)
        {
            dest=1;src=size-1;
        }
        else 
        {
            dest=myrank+1;src=myrank-1;
        }
        
        MPI::COMM_WORLD.Sendrecv(tosend.c_str(), length, MPI_CHAR, dest, 123, recvptr, CHUNKSIZE, MPI_CHAR, src, 123, status);
        

        string finalstr="";
        for (int i=0; i<size; i++)
        {
            if (i == myrank) {
                string recvstr;
                if(myrank !=0)
                {        
                    recvstr.assign(recvptr,recvptr+status.Get_count(MPI_CHAR)); 
                    finalstr = recvstr+trimstr; 
                }
                else finalstr=trimstr;
            }
            MPI::COMM_WORLD.Barrier();
        }
        
        delete(recvptr);
        str="";
        trimstr="";
        tosend="";

        process_string(finalstr, localmap, frequencymap);
        if(myrank ==0)
            std::cout<<"Processing file Ended: "<<(*it).c_str()<<endl;
    }
    MPI::COMM_WORLD.Barrier();
    if(myrank==0) cout<<" Time taken to process the files: "<< (clock()-fileprocessing_timestart)/(double) CLOCKS_PER_SEC<<"\n";

/*//Sending target words and getting firstlevelwords */
    auto it=localmap.begin();
    int maxlocalmapsize=0;
    int localmapsize = localmap.size();
    MPI_Allreduce(&localmapsize,    &maxlocalmapsize,    1,    MPI_INT,    MPI_MAX,    MPI_COMM_WORLD);    
    for(int l=0;l<maxlocalmapsize/NUM_OF_WORDS +1 ;l++)
    //for(int l=0;l<10 ;l++)
    {
        //copying 10 localmap element into another map;
        map_stringtostringint anothermap;
        for(int i=0;i<NUM_OF_WORDS;i++)
        {
            if(it!=localmap.end()){
                auto submap2 = it->second;
                anothermap[it->first]=submap2;
                it++;
            }
        }

        char *tenwords= new char[NUM_OF_WORDS*STRING_LENGTH];
        int k=0;
        for(auto &entry: anothermap)
        {
            string str(entry.first);                
            size_t length =str.copy(tenwords+k*STRING_LENGTH,STRING_LENGTH-1);
            *(tenwords+k*STRING_LENGTH+length)='\0';
            k++;
        }       
        while(k<NUM_OF_WORDS){ 
            *(tenwords+k*STRING_LENGTH)='\0';
            k++;
        }
        char *tenwordsfromall= new char[NUM_OF_WORDS*STRING_LENGTH*size];
        MPI_Allgather(tenwords,  NUM_OF_WORDS*STRING_LENGTH,  MPI_CHAR,  tenwordsfromall, NUM_OF_WORDS*STRING_LENGTH, MPI_CHAR,  MPI_COMM_WORLD);
        vector<string> searchstrings;
        for(int i=0;i<size;i++)
        {
            for(int k=0;k<NUM_OF_WORDS;k++)
                searchstrings.push_back((myrank^i)?string(tenwordsfromall+i*NUM_OF_WORDS*STRING_LENGTH+k*STRING_LENGTH):"");
        }

        
        delete[](tenwordsfromall);

        int totwords=0;
        int counts[size];
        
        auto me_wordtoword_ptr = get_wordtoword_instant(localmap,totwords,searchstrings,counts);

        int alltoallcounts[size];
        MPI_Alltoall( counts,  1,  MPI_INT,  alltoallcounts,  1,  MPI_INT,  MPI_COMM_WORLD);

        int senddisplacements[size];
        senddisplacements[0]=0;
        for(int i=1;i<size;i++)
            senddisplacements[i]=senddisplacements[i-1]+counts[i-1];

        int recvdisplacements[size];
        recvdisplacements[0]=0;
        for(int i=1;i<size;i++)
            recvdisplacements[i]=recvdisplacements[i-1]+alltoallcounts[i-1];

        int recvcount = recvdisplacements[size-1]+alltoallcounts[size-1];
        auto wordtoword_ptr = new Wordtoword[recvcount];
        MPI_Alltoallv(me_wordtoword_ptr, counts, senddisplacements, MPI_SingleWordtoWord, wordtoword_ptr, alltoallcounts, recvdisplacements, MPI_SingleWordtoWord, MPI_COMM_WORLD);
        delete[] me_wordtoword_ptr;

        reduce(wordtoword_ptr, recvcount, anothermap);  

        if(myrank==0)
        cout<<"Process: "<<myrank <<" FL Recv count:"<<recvcount<<endl;
        delete [] wordtoword_ptr;
    
// if(myrank==0){
//         long long c=0;
//         for(auto &entry: localmap)
//         {
//             c+=entry.second.size();
//         }
//         c+=localmap.size();
//     cout<<"Process: "<<myrank <<" localmap size:"<<localmap.size()<<" Elements: "<<c<<endl;
//     }
/*/////////////////////////////////////////////secondlevel stuff//////////////////////////////////////////////////*/ 
        
        //count how many first level words are there for the 10 target words
        std::set<string> dup;
        int firstlevelwordscount=0;
        for(int i=0;i<10;i++)
        {
            string str(tenwords+i*STRING_LENGTH);
            if(str.compare("")==0)continue;
            auto &submap = anothermap[str];
            for(auto &entry: submap){
                string str(entry.first); 
                dup.insert(str);         
            }            
        }
        firstlevelwordscount=dup.size();
        if(myrank==0)cout<<"Process: "<<myrank<<" "<<" FL word count: "<<firstlevelwordscount<<endl;

        //get all first level words
        char *firstlevelwords = new char[firstlevelwordscount*STRING_LENGTH];
        
        k=0;        
        for(auto entry: dup)
        {
            size_t length =entry.copy(firstlevelwords+k*STRING_LENGTH,STRING_LENGTH-1);
            *(firstlevelwords+k*STRING_LENGTH+length)='\0';
            k++;
        }
        cout<<"Process: "<<myrank<<" Came 1 ";

        //All_gather number of second level word counts
        int firstlevelcounts[size];
        MPI_Allgather(&firstlevelwordscount,  1,  MPI_INT,  firstlevelcounts,  1,  MPI_INT,  MPI_COMM_WORLD);
        cout<<"Process: "<<myrank<<" Came 2 ";
        int recvdisplacements_firstlevel[size];
        recvdisplacements_firstlevel[0]=0;
        for(int i=1;i<size;i++)
        {
            recvdisplacements_firstlevel[i]=recvdisplacements_firstlevel[i-1]+firstlevelcounts[i-1];
        }

        int recvsize = recvdisplacements_firstlevel[size-1]+firstlevelcounts[size-1];

        char *all_first_level_words = new char[recvsize*STRING_LENGTH];

        MPI_Allgatherv(firstlevelwords, firstlevelwordscount, MPI_Customword, all_first_level_words,  firstlevelcounts, recvdisplacements_firstlevel, MPI_Customword,  MPI_COMM_WORLD);
        cout<<"Process: "<<myrank<<" Came 3 ";
        
        vector<string> fl_strings;
        for(int i=0;i<recvsize;i++)
        {          
            fl_strings.push_back(string(all_first_level_words+i*STRING_LENGTH));
        }

        int sl_wordtoword_counts[size];
        auto sl_wordtoword_ptr = get_wordtoword_secondlevel_instant(localmap, fl_strings, firstlevelcounts, sl_wordtoword_counts, size);

        cout<<"Process: "<<myrank<<" Came 4 ";
        int sl_alltoallcounts[size];
        MPI_Alltoall( sl_wordtoword_counts,  1,  MPI_INT,  sl_alltoallcounts,  1,  MPI_INT,  MPI_COMM_WORLD);

        int sl_senddisplacements[size];
        sl_senddisplacements[0]=0;
        for(int i=1;i<size;i++)
        {
            sl_senddisplacements[i]=sl_senddisplacements[i-1]+sl_wordtoword_counts[i-1];
        }

        int sl_recvdisplacements[size];
        sl_recvdisplacements[0]=0;
        for(int i=1;i<size;i++)
        {
            sl_recvdisplacements[i]=sl_recvdisplacements[i-1]+sl_alltoallcounts[i-1];
        }
        recvcount=sl_recvdisplacements[size-1]+sl_alltoallcounts[size-1];
        cout<<"Process: "<<myrank<<" Came 5 ";
        auto sl_allwordtoword_ptr = new Wordtoword[recvcount];
	    if(myrank==0)cout<<"Process: "<<myrank<<" Receivecount of sl:"<<recvcount<<endl; 
        //MPI_Alltoallv(sl_wordtoword_ptr, sl_wordtoword_counts, sl_senddisplacements, MPI_SingleWordtoWord, sl_allwordtoword_ptr, sl_alltoallcounts, sl_recvdisplacements, MPI_SingleWordtoWord, MPI_COMM_WORLD);

        if( sl_wordtoword_ptr!=nullptr)delete [] sl_wordtoword_ptr;
        if(sl_allwordtoword_ptr !=nullptr)delete [] sl_allwordtoword_ptr;
        if(all_first_level_words !=nullptr)delete [] all_first_level_words;
        if(firstlevelwords !=nullptr)delete [] firstlevelwords;
        

        delete[](tenwords);
        }        
/*/////////////////////////////////////////////secondlevel stuff//////////////////////////////////////////////////*/

    

/*//////Read files in a loop and write initial data to localmap/////*/    
    
    
/*//////////broadcasting rootwords and getting the entries in other processes////////*/  /*  
    int localmapsize = localmap.size();
    
    int rootwordssent=0;
    std::unordered_map<string,std::unordered_map<string,int>>::iterator it = localmap.begin();
    
    
    for(int i=0;;i++)
    {
        i=i%size;
        int mapsize = localmap.size();
        int subwordscount=0;
        if(myrank==i)
        {
            if(rootwordssent<mapsize)
                subwordscount = (rootwordssent+ROUNDROBINSIZE<mapsize) ? ROUNDROBINSIZE : mapsize-rootwordssent;
            else
                subwordscount = 0;      
            cout<<"Process: "<<myrank<< " sending: " << subwordscount<<". out of: "<< mapsize;
        }
         char rootwords[ROUNDROBINSIZE][STRING_LENGTH];
        //broadcasting number of words
        MPI_Bcast(  &subwordscount,  1,  MPI_INT,  i,  MPI_COMM_WORLD);
        //everyone received the number of words the master process is going to send. if its 0, the iteration can be skipped.
        if(subwordscount!=0)
        {
            if(myrank==i)
            {      
                it = std::next(localmap.begin(), rootwordssent);
            
                for(int j=0;j< subwordscount;it++, j++)
                {
                    string str(it->first);                
                    std::size_t length = str.copy(rootwords[j],STRING_LENGTH-1);
                    rootwords[j][length]='\0';                
                }           
                rootwordssent+=subwordscount;
                cout<<" Rootwordssent: "<<rootwordssent;      
            }        
             
            //broadcasting all the words
            MPI_Bcast(  rootwords,  STRING_LENGTH*subwordscount,  MPI_CHAR,  i,  MPI_COMM_WORLD);                 
            int numofwordstosend=0;
            int numofwordfreqstosend=0;
            Wordtoword *wordtoword_ptr=nullptr;
            WordFrequency *wordfrequency_ptr=nullptr;

            if(myrank != i)
            {
                //get all the word entries matching in the local map
                wordtoword_ptr = get_wordtoword_ifpresent(localmap,numofwordstosend, rootwords,  subwordscount);  
                wordfrequency_ptr = get_wordfrequency_ifpresent(frequencymap,numofwordfreqstosend, rootwords, subwordscount);      
     
                //delete the matching entries
                removeentries(localmap,rootwords,subwordscount);
            }

            //sending the number of words to the broadcaster process
            int *recvcounts= new int[size];
            int *recvdisplacements= new int[size];   
            int *recvcountsfreq= new int[size];
            int *recvdisplacementsfreq= new int[size];        
            
            MPI_Gather(&numofwordstosend,  1,  MPI_INT,  recvcounts,  1,  MPI_INT,  i,  MPI_COMM_WORLD);
            MPI_Gather(&numofwordfreqstosend,  1,  MPI_INT,  recvcountsfreq,  1,  MPI_INT,  i,  MPI_COMM_WORLD);


            int totalwordsize=0;
            int totalwordsizefreq=0;
           
            Wordtoword *allwords=nullptr;
            WordFrequency *allwordsfreq=nullptr;
            if(myrank==i)
            {  
                recvdisplacements[0]=0;
                recvdisplacementsfreq[0]=0;

                for(int x=0;x<size;x++)
                {
                    if(x!=0)
                    {
                        recvdisplacements[x]=recvdisplacements[x-1]+recvcounts[x-1];
                        recvdisplacementsfreq[x]=recvdisplacementsfreq[x-1]+recvcountsfreq[x-1];
                    }
                    totalwordsize+=recvcounts[x];
                    totalwordsizefreq+=recvcountsfreq[x];
                }
                allwords = new Wordtoword[totalwordsize];
                allwordsfreq= new WordFrequency[totalwordsizefreq];

            }

            MPI_Gatherv(wordtoword_ptr, numofwordstosend, MPI_SingleWordtoWord, allwords,  recvcounts, recvdisplacements, MPI_SingleWordtoWord,i ,  MPI_COMM_WORLD);
            MPI_Gatherv(wordfrequency_ptr, numofwordfreqstosend, MPI_SingleWordFrequency, allwordsfreq,  recvcountsfreq, recvdisplacementsfreq, MPI_SingleWordFrequency,i ,  MPI_COMM_WORLD);
            
            if(myrank == i)
            {
                reduce(allwords, totalwordsize, localmap);  
                reducefreq(allwordsfreq, totalwordsizefreq, frequencymap);
            if(allwords!=nullptr)delete(allwords);
            if(allwordsfreq!=nullptr)delete(allwordsfreq);          
            }
            if(myrank!=i)
            {
                if(wordtoword_ptr!=nullptr)delete(wordtoword_ptr);      
                if(wordfrequency_ptr!=nullptr)delete(wordfrequency_ptr);
            }
            delete recvcounts;
            delete recvdisplacements;   
            delete recvcountsfreq;
            delete recvdisplacementsfreq; 
        }
        if(myrank==i)cout<<"\n";
        int arewedone=0;
        int numberofdone = mapsize-rootwordssent;
        if(numberofdone<0)numberofdone=0;
        MPI_Allreduce(&numberofdone,    &arewedone,    1,    MPI_INT,    MPI_SUM,    MPI_COMM_WORLD);
        if(arewedone==0){cout<<"Process: "<<myrank<< " came"<<"\n";
            break;}
    }
   
    int locsize = localmap.size();
    cout<<"Process: "<<myrank<<" Localmap size: "<<locsize<<endl;
    int totsize=0;
    MPI_Allreduce(&locsize, &totsize, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    if(myrank == 0) cout<<" Total size is: "<<totsize<<endl; */
/*//////////broadcasting rootwords and getting the entries in other processes////////*/
/*
    //----------------------------------------------------------------------------------------------------------
    ////broadcasting first level cooccurances and getting entries of second level coccurances from other processes
    //----------------------------------------------------------------------------------------------------------
    it=localmap.begin();
    int totalc=0;
    if(myrank==0){
        for(auto &entry: localmap){
            totalc+=(entry.second).size();
            cout<<(entry.second).size()<<" "<<entry.first<<"\t";
            for(auto &entry2: entry.second)
                cout<<entry2.first<<";";
            cout<<"\n";

        }
        cout<<"\n"<<totalc<<"\n";
    }

    while(1)
    {       
        std::clock_t whilestart=clock();   
        MPI::COMM_WORLD.Barrier();
        if(myrank==0)cout<<"while start lo"<<endl; 
        int totalwordsize=0;
        int totalwordsizefreq=0;            
        Wordtoword *allwords=nullptr;
        WordFrequency *allwordsfreq=nullptr;
        int subwordscount=0;
        for(int i=0;i<size;i++)
        {
            subwordscount=0;
            int mapsize = localmap.size();
            
            std::clock_t start=clock();
            std::set<string> uniquestrings;
            if(myrank==i)
            {
                for( ;it!=localmap.end();it++)
                {
                    if((it->second).size()==0)continue;  
                    cout<<(it->second).size()<<";";
                    for (auto &entry : it->second)
                    {                 
                        if(localmap.find(entry.first)==localmap.end()  && localsecondlevelmap.find(entry.first)==localsecondlevelmap.end())
                            uniquestrings.insert(entry.first);
                    }

                    subwordscount= uniquestrings.size();
                    //break doesnt increment it before quitting loop.
                    if(subwordscount>=2000){cout<<"\n";it++;break;}
                }
                //+mapsize;
                cout<< "Came1:"<<myrank<<" and subcount:"<<subwordscount;
            }
      
            int j=0;
               
            MPI_Bcast(  &subwordscount,  1,  MPI_INT,  i,  MPI_COMM_WORLD);
            if(subwordscount==0)continue;
            //char rootwords[subwordscount][STRING_LENGTH];
           char rootwords[subwordscount][STRING_LENGTH];        //char (*rootwords)[STRING_LENGTH] = new 
        
            if(myrank==i)
            {
                for (auto entry : uniquestrings)
                {
                    std::size_t length = entry.copy(rootwords[j],STRING_LENGTH-1);
                    rootwords[j][length]='\0';    
                    j++;        
                }
            }
            //broadcasting all the words
            MPI_Bcast(  rootwords, subwordscount,  MPI_Customword,  i,  MPI_COMM_WORLD);
            
            int numofwordstosend=0;
            int numofwordfreqstosend=0;
            Wordtoword *wordtoword_ptr=nullptr;
            WordFrequency *wordfrequency_ptr=nullptr;
          
            //sending the number of words to the broadcaster process
            if(myrank != i)
            {
                //get all the word entries matching in the local map
                wordtoword_ptr = get_wordtoword_ifpresent(localmap,numofwordstosend, rootwords,  subwordscount);   
                wordfrequency_ptr = get_wordfrequency_ifpresent(frequencymap,numofwordfreqstosend, rootwords, subwordscount);  
            }          
            
            int *recvcounts= new int[size];
            int *recvdisplacements= new int[size];
            MPI::COMM_WORLD.Barrier();
            int *recvcountsfreq= new int[size];
            int *recvdisplacementsfreq= new int[size];        
        
            MPI_Gather(&numofwordstosend,  1,  MPI_INT,  recvcounts,  1,  MPI_INT,  i,  MPI_COMM_WORLD);
            MPI_Gather(&numofwordfreqstosend,  1,  MPI_INT,  recvcountsfreq,  1,  MPI_INT, i,  MPI_COMM_WORLD);
            
            cout<<" STEP1 "<<(clock()-start)/ (double) CLOCKS_PER_SEC;
            if(myrank==i)
            {  
                recvdisplacements[0]=0;
                for(int x=0;x<size;x++)
                {
                    if(x!=0)
                    {recvdisplacements[x]=recvdisplacements[x-1]+recvcounts[x-1];}
                    totalwordsize+=recvcounts[x];         
                }
                allwords = new Wordtoword[totalwordsize];
                
                recvdisplacementsfreq[0]=0;          
                for(int x=0;x<size;x++)
                {
                    if(x!=0)
                    {
                        recvdisplacementsfreq[x]=recvdisplacementsfreq[x-1]+recvcountsfreq[x-1];
                    }
                    totalwordsizefreq+=recvcountsfreq[x];
                }
                allwordsfreq= new WordFrequency[totalwordsizefreq];
                cout<<" STEP1 "<<(clock()-start)/ (double) CLOCKS_PER_SEC<<" Totalwordssize:"<<totalwordsize;
            }
            
          
          
            MPI_Gatherv(wordtoword_ptr, numofwordstosend, MPI_SingleWordtoWord, allwords,  recvcounts, recvdisplacements, MPI_SingleWordtoWord,i ,  MPI_COMM_WORLD);
            MPI_Gatherv(wordfrequency_ptr, numofwordfreqstosend, MPI_SingleWordFrequency, allwordsfreq,  recvcountsfreq, recvdisplacementsfreq, MPI_SingleWordFrequency , i, MPI_COMM_WORLD);
            
                cout<<" STEP3 "<<(clock()-start)/ (double) CLOCKS_PER_SEC;
            if(recvcounts!=nullptr)delete recvcounts;
            if(recvdisplacements!=nullptr)delete recvdisplacements;
            if(recvcountsfreq!=nullptr)delete recvcountsfreq;
            if(recvdisplacementsfreq!=nullptr)delete recvdisplacementsfreq;
            if(wordtoword_ptr!=nullptr)delete wordtoword_ptr;
            if(wordfrequency_ptr!=nullptr)delete wordfrequency_ptr;    
                cout<<" STEP4 "<<(clock()-start)/ (double) CLOCKS_PER_SEC<<"\n";
        
            
        }
        cout<<"Process Memory: "<<myrank<<" Mapsize"<<mapsize(localmap)+mapsize(localsecondlevelmap)<<endl;
         if(allwords!=nullptr){
             reduce(allwords, totalwordsize, localsecondlevelmap,1);
             delete allwords;
         }
        cout<<"Process : "<< myrank <<" Reduce ended "<<(clock()-whilestart)/ (double) CLOCKS_PER_SEC<<endl;
         if(allwordsfreq!=nullptr){
             reducefreq(allwordsfreq, totalwordsizefreq, frequencymap,1);   
             delete allwordsfreq;
         }
         cout<<"Process Memory: "<<myrank<<" Mapsize"<<mapsize(localmap)+mapsize(localsecondlevelmap)<<endl;
         MPI::COMM_WORLD.Barrier();
         cout<<"Process: "<< myrank <<" While ended "<<(clock()-whilestart)/ (double) CLOCKS_PER_SEC<<endl;
        
        int arewedone=0;
        MPI_Allreduce(&subwordscount,    &arewedone,    1,    MPI_INT,    MPI_SUM,    MPI_COMM_WORLD);
        if(arewedone==0)break;
            
    }
    
    MPI::COMM_WORLD.Barrier();

    cout<<"Process: "<<myrank<<" writing to file started.."<<"\n";
     //----------------------------------------------------------------------------------------------------------
    ////writing to file
    //----------------------------------------------------------------------------------------------------------

    

    MPI_Offset offset = myrank*(2147483640/size);
    MPI_File file;
    MPI_Status status2;

    // opening one shared file
    char* filename1=new char[40];
    string filenamestr = "final1.txt";
    strcpy(filename1, filenamestr.c_str());
    MPI_File_open(MPI_COMM_WORLD, filename1, MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &file);

    // MPI_File_seek(file,0,MPI_SEEK_SET);
    // for(int l=1;l<myrank;l++){
    //     //MPI_File_seek(file, offset, MPI_SEEK_CUR);
    //     MPI_File_seek(file, offset, MPI_SEEK_CUR);
    // }

    char *line = new char[20];
    int x=0;
    int totallength=0;
    int maxlocalmapsize=0;
    localmapsize = localmap.size();
    MPI_Allreduce(&localmapsize,    &maxlocalmapsize,    1,    MPI_INT,    MPI_MAX,    MPI_COMM_WORLD);
     
     long long sizeofstr=0;
    int counter=0,counter1=0;
    it = localmap.begin();
    char *localstr= new char[20];
    int length=0;
    int length1=0;
    for(int ii=0;ii<maxlocalmapsize;ii++)
     {  
        if(it!=localmap.end() && (it->first).substr( (it->first).length() - 2 ) == "NN")  
        {   
            
            string rootentry ="";//= it->first +"    ";
             auto &submap = it->second;

            string secondlevelstring="(";
            for(auto& secondlevelentry : submap)
            {
                //if(secondlevelentry.second>=6)
                    secondlevelstring+=secondlevelentry.first+"::"+std::to_string(secondlevelentry.second)+",";
            }                        
            secondlevelstring.replace(secondlevelstring.end()-1,secondlevelstring.end(),")\n"); 
            rootentry+=it->first +"    "+it->first+"::"+ std::to_string(frequencymap[it->first]) + secondlevelstring;
                        
            for(auto &firstlevelentry : submap)
            {
                //if(firstlevelentry.second>=6)
                {
                    
                    //cout<<firstlevelentry.second<<"came"<<endl;
                    
                    if(localmap.find(firstlevelentry.first)!=localmap.end())
                    {   
                        auto &secondlevelsubmap = localmap[firstlevelentry.first];
                        string secondlevelstring="(";
                        
                        for(auto& secondlevelentry : secondlevelsubmap)
                        {
                            //if(secondlevelentry.second>=5)
                                secondlevelstring+=secondlevelentry.first+"::"+std::to_string(secondlevelentry.second)+",";
                        }     
                        secondlevelstring.replace(secondlevelstring.end()-1,secondlevelstring.end(),")\n"); 
                        rootentry+=it->first +"    "+firstlevelentry.first+"::"+ std::to_string(frequencymap[firstlevelentry.first]) + secondlevelstring;
                       
                    }
                    else if(localsecondlevelmap.find(firstlevelentry.first)!=localsecondlevelmap.end())
                    {
                        auto &secondlevelsubmap = localsecondlevelmap[firstlevelentry.first];
                        string secondlevelstring="(";
                        for(auto& secondlevelentry : secondlevelsubmap)
                        {
                            //if(secondlevelentry.second>=5)
                                secondlevelstring+=secondlevelentry.first+"::"+std::to_string(secondlevelentry.second)+",";
                        }
                        secondlevelstring.replace(secondlevelstring.end()-1,secondlevelstring.end(),")\n");
                        rootentry+=it->first +"    "+firstlevelentry.first+"::"+std::to_string(frequencymap[firstlevelentry.first]) +secondlevelstring;
                        
                    }
                    // else
                    //     if((it->first).compare("absence::NN")==0)counter1++;
                }
            }
            //rootentry+="\n";
            length = rootentry.length();
            free(localstr);
            
            localstr = new char[length+1];
            strcpy(localstr, rootentry.c_str());
            if(myrank==0)   
            //cout<<"maxlocalsize:"<<maxlocalmapsize<<"  localmapsize: "<<localmapsize<<"  counter: "<<counter  <<endl;                
            counter++;
        }else 
        {
            if(myrank==0)
            //cout<<"maxlocalsize:"<<maxlocalmapsize<<"  localmapsize: "<<localmapsize<<"  counter: "<<counter<<"------------over----------"  <<endl;
            counter++;
            length =0;
            free(localstr);
            localstr = new char[1];
            string nuller = "";
            strcpy(localstr,nuller.c_str());
            //it=localmap.end();
        }        
        if(myrank==0)sizeofstr+=length;
        MPI_File_seek(file, offset, MPI_SEEK_SET);
        MPI_File_write(file, localstr, length, MPI_CHAR, &status2);
        
        offset+=length;
        if(it!=localmap.end())
        std::advance(it, 1);
     }
    
     MPI_File_close(&file);
    
    MPI::COMM_WORLD.Barrier();


    if(myrank==0)cout<<"writing to file done..size="<<sizeofstr<<"\n";
    
    */
        
    MPI::Finalize(); 
    return 0; 
} 
