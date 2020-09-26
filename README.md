# MRFDA

A parallel version of Feature Decay Algorithms (FDA) using map-reduce technique.


## Configure

Configure the paths:
```
SPARK_PATH=/path/to/spark
PROJECT_PATH=/path/to/project
```
## Build


There are two programs to build: `createLuceneData` and`MRFDA`. Execute the following in each folder:

```
src package
```


## Execution


Download the Lucene jars

```
LUCENE_JARS=$PROJECT_PATH/lucenejars
cd $LUCENE_JARS
wget https://repo1.maven.org/maven2/org/apache/lucene/lucene-core/5.4.1/lucene-core-5.4.1.jar
wget https://repo1.maven.org/maven2/org/apache/lucene/lucene-analyzers-common/5.4.1/lucene-analyzers-common-5.4.1.jar
wget https://repo.typesafe.com/typesafe/maven-releases/com/typesafe/config/config/0.2.1/config-0.2.1.jar
```



### Store data using Lucene

```
pathFrom=$PROJECT_PATH/data/parallel_data.src
pathTo=$PROJECT_PATH/data/parallel_data.trg
lucenePath=$PROJECT_PATH/data/lucene_db
```

```
jar_list_notrim=$(ls -m $LUCENE_JARS/* | tr -d  ' ')
jar_list=$(echo $jar_list_notrim | tr -d ' ')
$SPARK_PATH/bin/spark-submit --jars $jar_list $PROJECT_PATH/createLuceneData/target/scala-2.12/createlucenedata_2.12-1.0.jar $pathFrom $pathTo $lucenePath
```


### Execute MRFDA

1. Convert the test set into n-grams:
```
python3 $SPARK_PATH/tools/toNgrams.py data/test_set 3 > data/ngram_set 
```
2. Edit `config` file:

```
lucenePath=/path/to/porject/data/lucene_db/data
langFrom=src 
langTo=trg
numSelectedData=100000
```
where `lucenePath` is the path where the data in lucene has been stored (add the `data` folder in the end). `langFrom` and `langTo` are the source and target languages (they are extracted from the extension of the parallel data, so use the same). Finally, `numSelectedData` is the number of sentences that MRFDA should retrieve.


3. Execute MRFDA:
```
$SPARK_PATH/bin/spark-submit --jars $jar_list $PROJECT_PATH/MRFDA/target/scala-2.12/mrfda_2.12-1.0.jar $PROJECT_PATH/config $PROJECT_PATH/data/ngram_set $PROJECT_PATH/data
```

##### Execute from shell

Other possible executions: use the shell, increasing the memory, executors ...

```
jar_list_notrim=$(ls -m $LUCENE_JARS/* | tr -d  ' ')
jar_list=$(echo $jar_list_notrim | tr -d ' ')
$SPARK_PATH/bin/spark-shell --jars $jar_list  --num-executors 4 --executor-cores 4 --executor-memory 50G --driver-memory 50G
```



## Citation

If you use this tool for academic research consider citing:

```
@article{poncelas2020improved,
  title={Improved feature decay algorithms for statistical machine translation},
  author={Poncelas, Alberto and de Buy Wenniger, Gideon Maillette and Way, Andy},
  journal={Natural Language Engineering},
  pages={1--21},
  publisher={Cambridge University Press}
}
```


