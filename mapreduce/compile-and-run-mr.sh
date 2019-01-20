DIR=/bmojo

cd $DIR

printf "\nCOPY FILE TO HDFS\n"

$HADOOP_PREFIX/bin/hdfs dfs -put bmojoall.tsv /bmojoall.tsv
$HADOOP_PREFIX/bin/hdfs dfs -put bmojoweek.tsv /bmojoweek.tsv

printf "\nSETTING EXECUTEABLE PY\n"

chmod a+x *.py

cd $HADOOP_PREFIX

printf "\nRUN HADOOP-STREAMING\n"

bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-$HADOOP_VERSION.jar \
    -input /bmojoall.tsv /bmojoweek.tsv \
    -output /bmojout \
    -mapper $DIR/mapper-bmojo.py \
    -reducer $DIR/reducer-bmojo.py

printf "\nRESULTS\n"

$HADOOP_PREFIX/bin/hdfs dfs -cat /bmojout/*
