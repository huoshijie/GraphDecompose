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

thesold=0.8
for i in 2000S 2000B 5000S 5000B
do
    scale=$i
    mu=0.1
    while [ `expr $mu \<= $thesold` -eq 1 ]
    do
        ./src/util/run_GraphDecompose_GraphPartition_CliqueBK.sh $scale/u$mu output 10 20
        mu=$[$mu+0.05]
    done
done


