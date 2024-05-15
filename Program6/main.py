# Program6: Write a MapReduce program using Java, to analyze the given employee record data and generate a statistics report with the total number of Female and Male Employees and their average salary.

from mrjob.job import MRJob

class MREmployee(MRJob):
    def mapper(self,_,line):
        row = line.split("\t")
        gender = row[3]
        salary = float(row[8])
        
        yield gender, salary
        
    def reducer(self,key,values):
        total_sal = 0
        cnt = 0
        
        for value in values:
            total_sal += value
            cnt += 1
            
        avg_sal = total_sal/cnt
        
        yield "Gender: " + key, "Total Employees: " + str(cnt) + " Average Salary: " + str(avg_sal)
        

if __name__ == '__main__':        
    MREmployee.run()
        