export LC_ALL="POSIX"
service ssh start

start-dfs.sh
start-yarn.sh

# test
cd /home/bdms/homework/hw2/part1
rm -f *.class *.jar
javac WordCount.java
jar cfm WordCount.jar WordCount-manifest.txt WordCount*.class

hdfs dfs -rm -f -r /hw2
hdfs dfs -mkdir /hw2
hdfs dfs -put example-input.txt /hw2
hadoop jar ./WordCount.jar /hw2/example-input.txt /hw2/output

hdfs dfs -cat '/hw2/output/part-*'

#  part1
cd /home/bdms/homework/mnt
cp ./part1/Hw2Part1.java ./hw2-check/0_202428015059020_hw2.java
cd hw2-check
rm ./score
./myprepare
./run-test-part1.pl ./score 0_202428015059020_hw2.java
cat ./score