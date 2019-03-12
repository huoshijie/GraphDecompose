:<<!
./src/util/run_GraphDecompose_GraphPartition_CliqueBK.sh SmallData/karate.txt output 20 20
./src/util/run_GraphDecompose_GraphPartition_CliqueBK.sh SmallData/dolphins.txt output 20 20
./src/util/run_GraphDecompose_GraphPartition_CliqueBK.sh SmallData/polbooks.txt output 20 20
./src/util/run_GraphDecompose_GraphPartition_CliqueBK.sh SmallData/netscience.txt output 20 20
./src/util/run_GraphDecompose_GraphPartition_CliqueBK.sh SmallData/email.txt output 30 20
./src/util/run_GraphDecompose_GraphPartition_CliqueBK.sh SmallData/CA-Grqc.txt output 30 40
./src/util/run_GraphDecompose_GraphPartition_CliqueBK.sh SmallData/CA-Hepth.txt output 30 20
./src/util/run_GraphDecompose_GraphPartition_CliqueBK.sh SmallData/PGP.txt output 30 20
!

scale=$1
mu=0.1
while [ $mu <= 0.8 ]
do
    ./src/util/run_GraphDecompose_GraphPartition_CliqueBK.sh SmallData/$scale/u$mu/network.txt output 10 20
    mu=$[$mu+0.05]
done