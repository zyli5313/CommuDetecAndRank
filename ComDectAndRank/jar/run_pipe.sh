# run whole pic pipeline

nmaxiter=22
#nnode=2879
# if data idx starts from 1, nnode = real # node + 1. Otherwise nnode = real # node
nnode=106
nreducer=20
ncluster=3
inpath='./pic_input/polbooks_feature.data'
taskid='pol'

./run_rownorm.sh $nreducer $inpath
./run_initvec.sh $nnode $nreducer $inpath
./run_pic.sh $nnode $nreducer $nmaxiter
./run_kmeans.sh $ncluster $nnode $nreducer $nmaxiter $taskid
