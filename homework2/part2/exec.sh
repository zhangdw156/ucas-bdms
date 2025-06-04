export LC_ALL="POSIX"
service ssh start
source /home/bdms/setup/GraphLite-0.20/bin/setenv
start-dfs.sh
start-yarn.sh

# test
cd /home/bdms/setup/GraphLite-0.20
start-graphlite example/PageRankVertex.so Input/facebookcombined_4w Output/out

# part2
cd /home/bdms/homework/mnt
cp ./part2/0_202428015059020_hw2.cc ./hw2-check/0_202428015059020_hw2.cc

cd hw2-check
rm ./score
./setup-test-part2.sh
./run-test-part2.pl ./score  0_202428015059020_hw2.cc
cat ./score