from pyspark import SparkContext,SparkConf
from operator import add
import math
import sys


class Problem:
    def Question(self,rdd,output,stopwords,k):
        stopword=stopwords.collect()
        k=int(k)
        def f1(x):
            list_1=[]
            list_2=[]
            list_3=[]
            for i in x:
                b=i.split(",")
                sj=b[0]
                sj=sj[0:4]
                shuju=b[1]
                for w in shuju.split(" "):
                    list_3.append(w)
                list_31=tuple(list_3)
                list_2.append(sj)
                list_2.append(list_31)
                list_21=tuple(list_2)
                list_1.append(list_21)
                list_2=[]
                list_3=[]
            return (list_1)
        a=rdd.collect()
        a=sc.parallelize(f1(a))
        
        def f2(x):
            (year,l)=x #()
            res=[]
            for i in l:
                res.append((year,i))
            return res
        a=a.flatMap(f2).filter(lambda x:x[1] not in stopword)
        noy=a.countByKey()
        numbyear=len(noy)

        def f3(x):
            re_1=[]
            for i in x.collect():
                tianjia=(i,1)
                re_1.append(tianjia)
            return re_1
        b=sc.parallelize(f3(a))
        qqq=b.groupByKey()
        tf_1=qqq.map(lambda x:(x[0],sum(x[1])))   #tf
        
        def f4(x):
            re_2=[]
            for i in x.collect():
                nian=i[0]
                count=i[1]
                n_1=nian[0] #2003
                t_1=nian[1]    #pig
                tianjia=(t_1,(n_1,count))
                re_2.append(tianjia)
            return re_2
        
        tf_z=sc.parallelize(f4(tf_1))
        df_1=a.distinct()
        
        def f5(x):
            re_2=[]
            for i in x.collect():
                n_2=i[0]
                t_2=i[1]
                tj_1=((t_2),(n_2))
                re_2.append(tj_1)
            return re_2
        df_2=sc.parallelize(f5(df_1))
        df_3=df_2.countByKey()

        def f6(x):
            re_3=[]
            for  k,v in x.items():
                tj_3=(k,v)
                re_3.append(tj_3)
            return re_3
        df_z=sc.parallelize(f6(df_3))
        idf_w1=tf_z.join(df_z)
        
        def f7(x):
            re_7=[]
            for i in x.collect():
                t_7=i[0]
                fen=i[1]
                df=fen[1]
                fen_1=fen[0]
                nian=fen_1[0]
                tf=fen_1[1]
                weight=tf*(math.log10(numbyear/df))
                weight_1=round(weight,6)
                tianjia=(nian,(t_7,weight_1))
                re_7.append(tianjia)
            return re_7
        
        idf_wz=sc.parallelize(f7(idf_w1))
        jg_1=idf_wz.sortByKey()
        jg_2=jg_1.sortBy(lambda x:(x[0],-x[1][1],x[1][0]),ascending=True)
        jg_3=jg_2.groupByKey().mapValues(lambda x: list(x))

        shuju_z=[]
        for i in jg_3.collect():
            nian=i[0]
            shuchu=i[1]
            list_z=[]
            for ii in range(k):
                n=shuchu[ii]
                d=""
                for iii in n:
                    d=d+str(iii)+","
                d=d.strip(",")
                list_z.append(d)
            dd=""
            for i in list_z:
                dd=dd+str(i)+";"
            dd=dd.strip(";") #zifu,shuju;
            shuju_z.append((nian,dd))
            
        shuju_z=sc.parallelize(shuju_z)
        shuju_z=shuju_z.sortByKey().map(lambda x:x[0]+'\t'+x[1])
        shuju_z.saveAsTextFile(output)
        sc.stop()
 
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
        
    conf = SparkConf().setAppName("problem3").setMaster("local")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile(sys.argv[1])
    output=sys.argv[2]
    stopwords=sc.textFile(sys.argv[3])
    k=sys.argv[4]
    p = Problem()
    p.Question(rdd,output,stopwords,k)
