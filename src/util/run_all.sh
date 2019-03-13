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
interval=0.05
for i in 10000S 10000B 20000S
do
    scale=$i
    for mu in 0.1 0.15 0.2 0.25 0.3 0.35 0.4 0.45 0.5 0.55 0.6 0.65 0.7 0.75 0.8
    do
        ./src/util/run_GraphDecompose_GraphPartition_CliqueBK.sh $scale/mu=$mu output 20 10
    done
done


