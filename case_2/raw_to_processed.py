# %%
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("myAppName").getOrCreate()

from os import listdir

# %%
class RawToProcessed:
    def __init__(self,spark,path_in,path_out):
        self.path_in = path_in
        self.path_out = path_out
        self.spark = spark
    def csv_to_parquet(self,filename_in,filename_out,**kwargs):
        df = self.spark.read.csv(f'{self.path_in}/{filename_in}',**kwargs)
        df.write.parquet(f'{self.path_out}/{filename_out}.parquet')

if __name__ == '__main__':
    # Definindo caminhos
    path_in = 'data/raw'
    path_out = 'data/processed'
    # Listando nomes
    files = listdir(path_in)
    names = [file.split('.')[0] for file in files]
    # Instanciando classe
    processor = RawToProcessed(spark,path_in,path_out)
    # Fazendo ETL 
    [processor.csv_to_parquet(file,name,sep = ';',header = True) for file,name in zip(files,names)]



