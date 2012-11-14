# run whole pic pipeline

nmaxiter=15
nnode=2879
nreducer=4
ncluster=2

./run_rownorm.sh $nreducer
./run_pic.sh $nnode $nreducer $nmaxiter
./run_kmeans.sh $ncluster $nnode $nreducer $nmaxiter