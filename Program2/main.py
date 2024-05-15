# Program2: Write a MapReduce program to analyze the given Weather Report Data and to generate a report with cities having maximum and minimum temperature for a particular year.

from mrjob.job import MRJob

class MRWeather(MRJob):
    def mapper(self,_,line):
        row = str(line)
        year = int(row[15:19])
        temp = float(row[87:92])
        
        yield year, temp
        
    def reducer(self, key, values):
        max_temp = -99999
        min_temp = 99999
        
        for value in values:
            if value > max_temp:
                max_temp = value
            if value < min_temp:
                min_temp = value
                
        yield "Year :"+str(key), ("Max_temp :"+str(max_temp),"Max_temp :"+str(min_temp))
            
    
if __name__ == '__main__':
    MRWeather.run()