from pyspark import SparkContext,SparkConf
from operator import add
import math
import sys


class Problem:
    def Question(self,input_1,input_2,tau,output):
        def f1(x):
            list1=[]
            list2=[]
            for e in x:
                l=e.split(" ")
                record_id=l[0]
                list1.append(l[1:])
                
                for i in list1:
                    list_c=[]
                    for ii in i:
                        if ii!="":
                            list_c.append(ii)
                    record_id=int(record_id)
                    n=(record_id,list_c)
                    list2.append(n)
                list1=[]
                # list2=[]
            return list2
            
        def f1_1(x):
            list_1=[]
            for i in x.collect():
                tianjia=(1,i)
                list_1.append(tianjia)
            return list_1

        def f1_2(x):
            list_1=[]
            for i in x.collect():
                tianjia=(2,i)
                list_1.append(tianjia)
            return list_1

        def citong(x):
            diccount = dict()
            for i in x:
                list_1=i[2]
                for ii in list_1:
                    if (ii not in diccount):
                        diccount[ii] = 1
                    else:
                        diccount[ii] = diccount[ii] + 1
            return diccount

        def zhakai(x,n,file_global):
            list_z=[]
            for i in x:
                x1=i[0]
                x2=i[1]
                x3=sorted(i[2],key=lambda i:(file_global.value[i],i))
                changdu=len(x3)
                if changdu!=0:
                    f_changdu=changdu-math.ceil(changdu*n)+1
                    x4=x3[0:f_changdu]
                    for ii in x4:
                        x5=(ii,(x1,x2,x3))
                        list_z.append(x5)
            return list_z

        def f_x(x,quezhi):
            list_z=[]
            for i in x:
                n=i[1]
                jishu_x=0
                if len(n)>1:
                    for ii in n: #n=[(1,0,['148']),(1,38,['10']),()]
                        wen_1=ii[0]
                        lie_1=ii[1]
                        jsl_1=ii[2]
                        jishu_x+=1
                        for iii in n[jishu_x:]:
                            wen_2=iii[0]
                            lie_2=iii[1]
                            jsl_2=iii[2]
                            if wen_1!=wen_2:
                                jsl_3=jsl_1+jsl_2
                                zong=len(jsl_3)
                                chong=len(set(jsl_3))
                                jieguo=((zong-chong)/chong)
                                if jieguo>=quezhi:
                                    jilu=format(jieguo,'.6f')
                                    jilu=float(jilu)
                                    if wen_1==1:
                                        xie=(lie_1,lie_2)
                                        xieru=(xie,jilu)
                                        list_z.append(xieru)
                                    else:
                                        xie=(lie_2,lie_1)
                                        xieru=(xie,jilu)
                                        list_z.append(xieru)
            return list_z
                    
        quezhi=float(tau)
        file_1=input_1.collect()
        file_1=sc.parallelize(f1(file_1))
        file_2=input_2.collect()
        file_2=sc.parallelize(f1(file_2))
        file_1=sc.parallelize(f1_1(file_1))
        file_1=file_1.map(lambda x: (x[0],x[1][0],x[1][1]))
        file_2=sc.parallelize(f1_2(file_2))
        file_2=file_2.map(lambda x: (x[0],x[1][0],x[1][1]))
        file_w=file_1.union(file_2).cache()
        file_w=file_w.collect()
        file_k=citong(file_w)
        file_global=sc.broadcast(file_k)  #sort
        file_z=zhakai(file_w,quezhi,file_global)
        file_z=sc.parallelize(file_z)
        file_z=file_z.groupByKey()
        file_z=file_z.mapValues(lambda x :list(x))
        file_z=file_z.collect()
        file_z=f_x(file_z,quezhi)
        file_z=sc.parallelize(file_z)
        file_z=file_z.distinct()
        file_z7=file_z.sortByKey(ascending=True)
        res_z=file_z7.map(lambda x:"("+str(x[0][0])+","+str(x[0][1])+")"+"\t"+ str(x[1]))
        
        res_z.saveAsTextFile(output)
        sc.stop()
 
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
        
    conf = SparkConf().setAppName("problem4").setMaster("local").set("spark.driver.memory", "8g").set("spark.driver.maxResultSize", "1g")

    sc = SparkContext(conf=conf)
    input_1 = sc.textFile(sys.argv[1])
    input_2 = sc.textFile(sys.argv[2])
    tau=sys.argv[3]
    output=sys.argv[4]
    p = Problem()
    p.Question(input_1,input_2,tau,output)

