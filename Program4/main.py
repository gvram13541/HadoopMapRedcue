# Program4: Write a MapReduce program to analyze the given Insurance Data and generate a statistics report with the construction building name and the count of building/county name and its frequency.

from mrjob.job import MRJob
import csv

class MRInsurance(MRJob):
    def mapper(self,_,line):
        row = next(csv.reader([line])) 
        name = row[2]
        yield name, 1
        
    def reducer(self, key, values):
        yield "Building Name: " + key,  "Frequency: " + str(sum(values))
        
        
if __name__ == '__main__':
    MRInsurance.run()