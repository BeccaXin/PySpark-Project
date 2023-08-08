import re
from mrjob.job import MRJob
from mrjob.step import MRStep

class proj1(MRJob):
   
    def mapper_1(self, _, line):
        words = re.split(",", line.lower())

        for i in range(0, len(words)-2):
            if len(words[i]):
                for j in range(i+1, len(words)-1):
                    if len(words[j]):
                        yield words[i]+",*", 1
                        yield words[i]+","+words[j], 1
                       
    def combiner_1(self, key, values):
        yield key, sum(values)
            
    def reducer_init_1(self):
        self.marginal = 2
            
    def reducer_1(self, key, values):
        wi, wj = key.split(",")    #u1,;1
        if wj=="*":
            self.marginal = sum(values)
        else:
            count = sum(values)
            r=count/self.marginal
            result=round(r,4)
            yield key+","+str(result),1   #u1,l1,0.6666
    
           
    def mapper_2_init(self):
        self.key_list=[]
        self.values_2=[]

    def mapper_2(self,key,values):
        key_2=key.split(",")#u1,l1,0.666
        key_22=key_2[0]    #k_22-u1
        key_21=key_2[1]    #k_k21-l1
        key_23=key_2[2]
        self.key_list.append(key_21)  #l1,u1,values
        self.key_list.append(key_22)
        self.key_list.append(key_23)
        if len(self.key_list):
            kk1=str(self.key_list[0])
            kk2=str(self.key_list[1])
            value_s=str(self.key_list[2])
            self.key_list=[]
            self.values_2=[]
            yield kk1 +","+value_s+","+kk2,1

    def reducer_init_2(self):
        self.ki_2=[]

    def reducer_2(self,key,values):
        kkk=key.split(",")    #l1,0.66,u1
        kkkk_0=""
        kkkk_1=""
        kkkk_2=""
        for i in kkk:
            self.ki_2.append(i)
        if len(self.ki_2):
            kkkk_0=self.ki_2[0]  #kk1
            kkkk_1=self.ki_2[1]  #kk1
            kkkk_2=self.ki_2[2]  #values
            self.ki_2=[]
            yield kkkk_0,kkkk_2+","+kkkk_1
#
    SORT_VALUES = True
            
    JOBCONF_1 = {
        'mapreduce.map.output.key.field.separator':',',
        'partitioner': 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner',
        'mapreduce.partition.keypartitioner.options':'-k1,1',
        'mapreduce.job.output.key.comparator.class':
        'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator'
        
    }
    
    JOBCONF_2= {
        'mapreduce.map.output.key.field.separator': ',',
        'partitioner': 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner',
        'mapreduce.partition.keypartitioner.options':'-k1,1',
        'mapreduce.job.output.key.comparator.class':
        'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
        'mapreduce.partition.keycomparator.options': '-k1,1 -k2,2nr'

    }
    def steps(self):
        return [
            MRStep(mapper=self.mapper_1,
            combiner=self.combiner_1,
            reducer_init=self.reducer_init_1,
            reducer=self.reducer_1,
            jobconf=self.JOBCONF_1),
            
            MRStep(mapper_init=self.mapper_2_init,
            mapper=self.mapper_2,
            reducer_init=self.reducer_init_2,
            reducer=self.reducer_2,jobconf=self.JOBCONF_2)

        ]

if __name__ == '__main__':
    proj1.run()
    
