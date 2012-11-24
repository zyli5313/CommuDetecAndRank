# run whole pic pipeline

nmaxiter=22
#nnode=2879
# if data idx starts from 1, nnode = real # node + 1. Otherwise nnode = real # node
nnode=106
nreducer=15
ncluster=3
inpath='./pic_input/polbooks_feature.data'
taskid='pol'
thresbase=0.001

# usage: RowNorm <edge_path> <output_path> <# of reducers> <makesym or nosym>  <taskid>
hadoop jar pic.jar pic.RowNorm $inpath ./pic_rownorm $nreducer nosym $taskid

# PicInitVec <edge_path> <output_path> <# of nodes> <# of reducers> <makesym or nosym> <start1 or start0>  <taskid>
hadoop jar pic.jar pic.InitVec $inpath ./pic_out $nnode $nreducer nosym start1 $taskid

#usage: Pic <edge_path> <base threshold> <output_path> <# of nodes>  <# of tasks> <max iteration> <makesym or nosym> <new or contNN>  <start0 or start1>  <taskid>
hadoop jar pic.jar pic.Pic ./pic_rownorm $thresbase ./pic_iterout $nnode $nreducer $nmaxiter nosym new start1 $taskid

# 1: warp pic input to feed kmeans
# 2: generate inital cluster center for kmeans
# 3: kmeans

# Usage: InputWrapper <input_path> <output_path> <# of tasks> <taskid>
hadoop jar pic.jar kmeans.InputWrapper ./pic_out ./pic_km_in $nreducer $taskid

# Usage: InitVec <input_path> <output_path> <# of clusters> <# of nodes> <# of tasks> <taskid>
hadoop jar pic.jar kmeans.InitVec ./pic_km_in ./pic_km_initvec $ncluster $nnode $nreducer $taskid

# Usage: Kmeans <in> <out> <init> <# of clusters> <# of nodes> <# reducers> <nooutkey or outkey> <taskid>  <outdraw or nooutdraw>
hadoop jar pic.jar kmeans.Kmeans ./pic_km_in ./pic_km_out ./pic_km_initvec $ncluster $nnode $nreducer $nmaxiter outkey $taskid outdraw
rm /h/zeyuanl/code/DMProj/result/pic_km_out_$taskid.txt
hadoop fs -getmerge ./pic_km_out_$taskid /h/zeyuanl/code/DMProj/result/pic_km_out_$taskid.txt
