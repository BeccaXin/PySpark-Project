from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import sys

class Project:

    def Question(self,inputdf,outputdf,stopwords,k):
        field_stopwords= spark.read.text(stopwords).toDF("stopword")
        field_1=spark.read.text(inputdf).toDF("content")
        
        g=k
        stopword=field_stopwords.select(collect_set('stopword').alias('stopword')).first()['stopword']
        stopword=[]
        field_1=field_1.withColumn("terms",slice(split(split(field_1['content'],',')[1],' '),1,10000))

        field_2=field_1.select('content',explode('terms').alias('term'))
        field_2=field_2.filter(~field_2['term'].isin(stopword))

        field_6=field_2.withColumn('year',split(field_1['content'],',')[0])
        field_6 = field_6.withColumn('year',expr('substring(year, 0,4)'))

        field_3=field_6.groupBy('term','year').agg(countDistinct('content').alias('tf'))
        field_5=field_6.groupBy('term').agg(countDistinct('year').alias('df'))
        field_6=field_5.join(field_3,on='term')

        df1 = field_6.dropDuplicates(subset=[c for c in field_6.columns if c in ["year"]])
        numofyear=df1.count()
        idf=field_6.select('term','year',(field_6['tf']*log10(numofyear/field_6['df'])).alias('idf'))
        # idf.show(1000)
        field_7=idf.orderBy(['year','idf','term'],ascending=[1,0,1])

        field_7= field_7.select('year','term',bround("idf", scale=6).alias('idf_z'))
        field_8=field_7.select('year','term','idf_z').dropDuplicates()
        field_8=field_8.orderBy(['year','idf_z','term'],ascending=[1,0,1])

        #sort

        window = Window.partitionBy(field_8['year']).orderBy(field_8['idf_z'].desc())
        field_9=field_8.withColumn('top',row_number().over(window))
        field_9=field_9.where(field_9.top<=g)
        rdd=field_9.rdd

        list_2=[]
        for i in rdd.collect():
            n=1
            for ii in i:
                if n==1:
                    nian=ii
                if n==2:
                    shu=ii
                if n==3:
                    weight=ii
                n+=1

            list_1=[nian,shu,weight]
            list_2.append(list_1)
        kk=int(k)
        n=1
        shuju=""
        list_3=[]
        for i in list_2:
            nian=str(i[0])
            terms=str(i[1])
            weight=str(i[2])
            shuju=shuju+terms+","+weight+";"
            if n==kk:
                shuju=nian+"\t"+shuju
                shuju=shuju.strip(";")
                list_3.append(shuju)
                shuju=''
                n=0
            n+=1
        shuju_z=sc.parallelize(list_3)
        shuju_z.repartition(1).saveAsTextFile(outputdf)
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
        
    
    spark = SparkSession.builder.master("local[*]").appName("problem").getOrCreate()
    sc=spark.sparkContext
    inputdf= sys.argv[1]
    outputdf=sys.argv[2]
    stopwords=sys.argv[3]
    k=sys.argv[4]
    p = Project()
    p.Question(inputdf,outputdf,stopwords,k)
    

