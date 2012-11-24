#!/bin/bash
# 1: warp pic input to feed kmeans
# 2: generate inital cluster center for kmeans
# 3: kmeans

# param
ncluster=$1
nnode=$2
nreducer=$3
nmaxiter=$4
taskid=$5

# Usage: InputWrapper <input_path> <output_path> <# of tasks> <taskid>
hadoop jar pic.jar kmeans.InputWrapper ./pic_out ./pic_km_in $nreducer $taskid

# Usage: InitVec <input_path> <output_path> <# of clusters> <# of nodes> <# of tasks> <taskid>
hadoop jar pic.jar kmeans.InitVec ./pic_km_in ./pic_km_initvec $ncluster $nnode $nreducer $taskid

# Usage: Kmeans <in> <out> <init> <# of clusters> <# of nodes> <# reducers> <nooutkey or outkey> <taskid>  <outdraw or nooutdraw>
hadoop jar pic.jar kmeans.Kmeans ./pic_km_in ./pic_km_out ./pic_km_initvec $ncluster $nnode $nreducer $nmaxiter outkey $taskid outdraw 
rm /h/zeyuanl/code/DMProj/result/pic_km_out_$taskid.txt
hadoop fs -getmerge ./pic_km_out_$taskid /h/zeyuanl/code/DMProj/result/pic_km_out_$taskid.txt
