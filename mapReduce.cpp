#include "mpi.h"
#include <iostream>
#include <math.h>
#include <stdio.h>
#include <bits/stdc++.h>

//Opening and reading directory
#include "sys/types.h"
#include "dirent.h"

//Storing Index
#include <unordered_map>
#include <map>

#include <string>
#include <iterator>

//For sprintf
#include <string.h>

//File IO
#include <sstream>
#include <fstream>

//For sorting vector
#include <algorithm>

//cereal libraries
#include "cereal/types/map.hpp"
#include "cereal/types/vector.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/archives/binary.hpp"

using namespace std;

/************************************************************************************************************************************
To compile: mpiCC -Iinclude filename -std=c++11
To run: Give the path to the folder containing the data as an argument
************************************************************************************************************************************/

#define MAX_FILE_NAME_SIZE 100
#define LOWEST_ALPHABET_ASCII 97
#define LONG_DIGIT_NO 10

//Comparator function to sort vectors in descending order
bool sortinrev(const pair<long int,string> &a, const pair<long int,string> &b)
{
       return (a.first > b.first);
}

//Merge two sorted vectors into one again in descending order 
vector<pair<long int, string>> mergeVectors(vector<pair<long int, string>> vec1,vector<pair<long int, string>> vec2)
{
	vector<pair<long int, string>> mergedVec(vec1.size()+vec2.size());
	merge(vec1.begin(),vec1.end(),vec2.begin(),vec2.end(),mergedVec.begin(),sortinrev);
	vec1.clear();
	vec2.clear();

	return mergedVec;
}

/*********************** ***********UTILITY FUNCTION FOR PRINTING LOCAL INDEX INTO FILES******************************************/

//Function to print vector of maps
void printMaps(vector<unordered_map<string, vector<pair<long int,string>>>> receivedMaps, int noOfProcesses, int processId, string globalIndexFolder)
{
	unordered_map<string, vector<pair<long int,string>>> final_map;
	unordered_map<string, vector<pair<long int,string>>>::iterator mapItr;

	//Postings list(a list of freq + docID) for a word
	vector<pair<long int,string>> documentsWithWord,mergedVector;

	//An entry for a map -> word and its postings list
	string currentWord;
	vector<pair<long int , string>> postingsForWord;

	char filename[MAX_FILE_NAME_SIZE];
	long int wordFreq;
	string documentID;

	sprintf(filename,"%s/%d.txt",globalIndexFolder.c_str(),processId);

	FILE* fp = fopen(filename,"w");

	vector<pair<long int, string>>::iterator itr;

	fprintf(fp,"\n--------------Index Begin--------------\n\n");

	for(int i=0;i<noOfProcesses;i++)
	{
		final_map = receivedMaps[i];

		fprintf(fp, "Partition %d ===> \n\n",i);

		//Iterate over FINAL MAP
		for(mapItr = final_map.begin(); mapItr!= final_map.end();mapItr++)
		{	

			//Get one entry from a map
			currentWord = mapItr->first;
			postingsForWord = mapItr->second;

			fprintf(fp,"%s:\n",currentWord.c_str());

			for(itr = postingsForWord.begin(); itr!=postingsForWord.end(); itr++)
			{
				documentID = itr->second;
				wordFreq = itr->first;
				fprintf(fp, "\t%s : %ld \n", documentID.c_str(), wordFreq);
			}	
		
		}
	}

	fprintf(fp,"\n\n--------------Index Over--------------\n\n");

	fclose(fp);
}


/***********************************************************************************************************************/


int main(int argc, char** argv)
{
	int err;
	int processId,noOfProcesses;
	string currentFolder = ".";
	string parentFolder = "..";
	
	//To calculate total time and the time required to make local indices
	double elapsedTime,elapsedTimeLocal;

	//Word Count Map for each document : stores a word and its frequency in the document
   	unordered_map<string, long int> wordCountMap;

	//Parallelization starts
	err = MPI_Init(&argc, &argv);

	//Get Rank and Total no of processes
	err = MPI_Comm_rank(MPI_COMM_WORLD, &processId);
	err = MPI_Comm_size(MPI_COMM_WORLD, &noOfProcesses);
	
	//Time calculation starts
	elapsedTime = -MPI_Wtime();
	elapsedTimeLocal = -MPI_Wtime();


/********************************************Mapping Phase Begins*******************************************************/	
   	
   	//A vector of Inverted Index Maps for whole documents in the local node
   	vector<unordered_map<string, vector<pair<long int,string>>>> invertedIndexMap(noOfProcesses);

	//Load Stopwords into the set
	//https://algs4.cs.princeton.edu/35applications/stopwords.txt//
	unordered_set<string> stopwords;
	ifstream stopwordsStream;
	stopwordsStream.open("stopwords.txt");
	string stopWord;		

	while(getline(stopwordsStream, stopWord))
	{
	   	if(stopWord.empty())
	   	{
	   		continue;
	   	}	 

	   	if (stopWord[stopWord.size()-1]=='\n')
	   	{
	   		stopWord.erase(stopWord.size()-1, 1);
	   	}					        
	   	
	   	stopwords.insert(stopWord);
	}	
	
	//Stores the name of file to be opened for indexing
	char filename[MAX_FILE_NAME_SIZE];

	//There is a directory for each node
	char directoryName[MAX_FILE_NAME_SIZE];
	char* directory = argv[1];
	sprintf(directoryName,directory);

	DIR *dp; 
	struct dirent *ep;    
	 
	dp = opendir(directoryName);

	//Partitioning Words according to the first letter of the word 
	int partitionSize = 26/(noOfProcesses);
	int remainder = 26%(noOfProcesses);
	vector<pair<long int,string>>::iterator vecItr;
	
	//array containing the partition index (0...(noOfProcesses)) for each alphabet
	int partitionIndex[26];

	int low,high;

	for(int k=0;k<(noOfProcesses);k++)
	{
		low = partitionSize*k;
		high = partitionSize*(k+1) - 1;
		if(k == (noOfProcesses-1))
			high = high + remainder;

		for(int j=low;j<=high;j++)
		{
			partitionIndex[j] = k;
		}
	}

	//Partition to which a given word belongs
	int correspondingPartition;

	//Variable declarations
	long int documentNo, wordFreq;
	ifstream inputFile;
	string readLine;
	int len;	
 	string currentWord;
    unordered_map<string, long int>::iterator mapItr;
    vector<pair<long int,string>> documentsWithWord,mergedVector;
    string documentID;
    
	if (dp != NULL)
	{
		documentNo = 0;

		//Start creating the local index in the node -- read each file one by one
		while ((ep = readdir(dp))!=NULL)
		{

			//Ignore the current and parent folders
			if(((string(ep->d_name)).compare(currentFolder))==0 || ((string(ep->d_name)).compare(parentFolder))==0)
			{
				continue;
			}

			documentNo++;			
			stringstream s;
			s << processId << "-" << documentNo;
			documentID = s.str();

			//Deallocating Memory
			s.str().clear();

			//Current file read
			sprintf(directoryName,directory);
			sprintf(filename,"%s/%s",directoryName,ep->d_name);
			inputFile.open(filename);

		 	while(getline(inputFile, readLine))
			{
			    if(readLine.empty())
			    {    
			        continue;
			    }

			    char* lineString = &readLine[0];
			   	
			   	//Tokenize the line using punctuation marks
			    char* token = strtok(lineString,",./;-!?@&(){}[]<>:'\" \r");

			    while(token != NULL)
			    {
			        for(long int k=0;k<strlen(token);k++)
			        {
			            token[k] = tolower(token[k]);
			        }

			        string str(token); 
			        if((token[0]>='a' && token[0]<='z')||(token[0]>='A' && token[0]<='Z')||(token[0]>='0' && token[0]<='9'))
			        {
			            //Check for stopword
			            if(stopwords.find(str)==stopwords.end())
			            {
			                //Update its frequency
			            	wordCountMap[str]++;
			            }             
			        }
			        token = strtok(NULL,",./;-!?@&(){}[]<>:'\" \r");
			    }
			}

   			//Closing the file
   			inputFile.close();

   			//Here the document is processed and the file is closed

		    //Start adding the words from the word count map to the inverted index map
		    for (mapItr= wordCountMap.begin(); mapItr != wordCountMap.end(); mapItr++)
		    {
	        	currentWord = mapItr->first;
	        	wordFreq = mapItr->second;

	        	//If the first letter is a number then put the word into last partition since last partition generally contains less words
	        	if(currentWord[0] >= 48 && currentWord[0] <= 57)
	        		correspondingPartition = noOfProcesses-1;
	        	else
	        		correspondingPartition = partitionIndex[currentWord[0]-LOWEST_ALPHABET_ASCII];
				
	        	//If the currentWord doesn't exist in invertedIndexMap
	        	if(invertedIndexMap[correspondingPartition].find(currentWord) == invertedIndexMap[correspondingPartition].end())
	        	{
	        		//Create a new vector corresponding to that word
	        		vector<pair<long int,string>> newVector;
	        		newVector.push_back(make_pair(wordFreq,documentID)); 
	        		invertedIndexMap[correspondingPartition][currentWord] = newVector;
	        	}
	        	//Otherwise make the document ID entry for the current word in its already existing map
	        	else
	        	{
	        		documentsWithWord = invertedIndexMap[correspondingPartition][currentWord];
	        		documentsWithWord.push_back(make_pair(wordFreq,documentID));
	        		invertedIndexMap[correspondingPartition][currentWord] = documentsWithWord;
	        	}
	        }

	        //Clearing the wordCountMap for processing new document
	        wordCountMap.clear();

	        //Now go to the next document in next iteration
		}
	    
	    //Close the directory
      	closedir (dp);
    }
	else
	{
	    perror("Couldn't open the directory");
	}

	//Local Indices extraction done
	elapsedTimeLocal += MPI_Wtime();

	//Print time taken to extract local indices
	printf("Local Index time: %lf\n",elapsedTimeLocal);

	/****************************************************Sorting the vectors in the local Map******************************************/

	unordered_map<string, vector<pair<long int,string>>>::iterator mapItr1;
	for(int k=0;k<(noOfProcesses);k++)
	{
		for(mapItr1=(invertedIndexMap[k]).begin();mapItr1!=(invertedIndexMap[k]).end();mapItr1++)
		{
			//sort the vectors containing word frequency along with document ID according to frequency for each word in invertedIndexMap
			sort((mapItr1->second).begin(), (mapItr1->second).end(), sortinrev);
		}
	}

	//*******************************************ALL DOCUMENTS IN NODE ARE PROCESSESED****************************************//


	/************************************************** COMMUNICATION BEGINS ***********************************************/
	
	//******************** if documents size less ************************//
	if(0)
	{
		//Here the communication uses serialized strings

		//String Stream to store the serialized archive of the maps
		stringstream ss;
		cereal::BinaryOutputArchive outArchive(ss);

		//Array containing the serialized string for maps corresponding to each partition
		string serializedMap[noOfProcesses];
		int serializedStringSizes[noOfProcesses];

		//All strings concatenated to form on combined string
		string combinedSerializedMapString;

		//Displacement from start location from where to take the data for sending/receiving to/from each process
		int sendDisp[noOfProcesses];
		int receiveDisp[noOfProcesses];

		//Size of data received from each process
		int receiveSizes[noOfProcesses];

		//Total data to be received
		int totalReceiveSize = 0;
		int previousStringSize = 0;

		for(int k=0;k<noOfProcesses;k++)
		{
			//Serialize the map
			outArchive(invertedIndexMap[k]);

			//Extract the string from the string stream
			serializedMap[k] = ss.str();
			serializedStringSizes[k] = serializedMap[k].size();	
			combinedSerializedMapString += serializedMap[k];
		
			sendDisp[k] = previousStringSize;
			previousStringSize += serializedStringSizes[k];
			
			//Clear the string stream
			ss.str("");
			ss.clear();
		}
		
		//Send/Receive the sizes of data(length of the string) to be sent/received to/from each process
		MPI_Alltoall(&(serializedStringSizes[0]), 1, MPI_INT, &(receiveSizes[0]), 1, MPI_INT, MPI_COMM_WORLD);

		previousStringSize = 0;
		for(int k=0;k<noOfProcesses;k++)
		{
			totalReceiveSize += receiveSizes[k];
			receiveDisp[k] = previousStringSize;
			previousStringSize += receiveSizes[k];	
		}
		
		//Combined string to receive the strings from each process
		string receivedSerializedMapString;
		receivedSerializedMapString.resize(totalReceiveSize);

		//Send/Receive strings to/from all other processes
		MPI_Alltoallv(&(combinedSerializedMapString[0]), &(serializedStringSizes[0]), &(sendDisp[0]), MPI_CHAR, &(receivedSerializedMapString[0]), &(receiveSizes[0]), &(receiveDisp[0]), MPI_CHAR, MPI_COMM_WORLD);

		previousStringSize = 0;

		//String stream to extract the maps
		stringstream newSS;
		cereal::BinaryInputArchive inArchive(newSS);	
		for(int k=0;k<noOfProcesses;k++)
		{
			newSS << receivedSerializedMapString.substr(previousStringSize,receiveSizes[k]);

			//Extract the map from the serialized string
			cereal::BinaryInputArchive inArchive(newSS);
			inArchive(invertedIndexMap[k]);
			previousStringSize += receiveSizes[k];
			newSS.str("");
			newSS.clear();
		}	
	}

	//**************** end ************************//

	//**************** large database ************************//
	else
	{
		//Here the communication uses file I/O

		//File stream to store the serialized maps in binary form
		ofstream serializeFile;
		cereal::BinaryOutputArchive outArchive(serializeFile);
		
		for(int k=0;k<noOfProcesses;k++)
		{
			serializeFile.open("SerializedFiles/"+to_string(k)+to_string(processId), std::ofstream::out | std::ofstream::trunc);
			outArchive(invertedIndexMap[k]);
			serializeFile.close();
		}
		
		//Wait till all the processes are done making their respective files containing serialized maps
		MPI_Barrier(MPI_COMM_WORLD);

		ifstream inputSerializeFile;
		cereal::BinaryInputArchive inArchive(inputSerializeFile);
		for(int k=0;k<noOfProcesses;k++)
		{
			inputSerializeFile.open("SerializedFiles/"+to_string(processId)+to_string(k));
			inArchive(invertedIndexMap[k]);
			inputSerializeFile.close();
		}
	}

	//**************** end ************************//

	
	/*****************************************************REDUCE PHASE BEGINS******************************************************/
	
	unordered_map<string, vector<pair<long int,string>>> final_map, localMap;
	
	//Maps received from all processes
	vector<unordered_map<string, vector<pair<long int,string>>>> receivedMaps = invertedIndexMap;
	
	//An entry for a map -> word and its postings list
	vector<pair<long int , string>> postingsForWord;


	//Iterate over all maps
	for(int k = 0; k<noOfProcesses;k++)
	{

		localMap = receivedMaps[k];

		//Iterate over one map
		for(mapItr1 = localMap.begin(); mapItr1!= localMap.end(); mapItr1++)
		{
			
			//Get one entry from a map
			currentWord = mapItr1->first;
			postingsForWord = mapItr1->second;

			//If it doesn't exists in the final map, make a new entry
			if(final_map.find(currentWord) == final_map.end())
			{
				final_map[currentWord] = postingsForWord;
			}

	       	//Otherwise merge the existing posting list with this one
	       	else
	       	{
	       		documentsWithWord = final_map[currentWord];
	       		mergedVector = mergeVectors(documentsWithWord, postingsForWord);
	       		final_map[currentWord] = mergedVector;
	       	}

		}	

	}

	//Total time calculation done (global indices computed)
	elapsedTime += MPI_Wtime();

	/*****************************************************Final Map Ready*********************************************************/

	//Now write it into file

	//Assuming the distributed global index files are in a folder and file name will be the process id

	//Name of the folder
	string globalIndexFolder = "GlobalIndex";
	
	sprintf(filename,"%s/%d.txt",globalIndexFolder.c_str(),processId);

	FILE* fp = fopen(filename,"w");

	vector<pair<long int, string>>::iterator itr;

	fprintf(fp,"\n--------------Index Begin--------------\n\n");

	//Iterate over FINAL MAP
	for(mapItr1 = final_map.begin(); mapItr1!= final_map.end();mapItr1++)
	{	
		//Get one entry from a map
		currentWord = mapItr1->first;
		postingsForWord = mapItr1->second;

		fprintf(fp,"%s:\n",currentWord.c_str());

		for(itr = postingsForWord.begin(); itr!=postingsForWord.end(); itr++)
		{
			documentID = itr->second;
			wordFreq = itr->first;
			fprintf(fp, "\t%s : %ld \n", documentID.c_str(), wordFreq);
		}	
	
	}

	fprintf(fp,"\n\n--------------Index Over--------------\n\n");

	fclose(fp);
	
	printf("Total time: %lf\n",elapsedTime);

	err = MPI_Finalize();
	return 0;
		
}

