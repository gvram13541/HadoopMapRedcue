# Program6: Write a MapReduce program using Java/Python, to print the frequency of each word in the given text


from mrjob.job import MRJob

class WordCount(MRJob):
    def mapper(self,_,line):
        row = line.split()
        
        for word in row:
            yield word, 1
        
    def reducer(self,key,values):
        yield key, sum(values)
        

if __name__ == '__main__':        
    WordCount.run()
        