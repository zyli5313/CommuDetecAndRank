#!/bin/bash
# 1: warp pic input to feed kmeans
# 2: generate inital cluster center for kmeans
# 3: kmeans

# Usage: InputWrapper <input_path> <output_path> <# of tasks>
#hadoop jar pic.jar kmeans.InputWrapper ./pic_out ./pic_km_in 4

# Usage: InitVec <input_path> <output_path> <# of clusters> <# of nodes> <# of tasks>
#hadoop jar pic.jar kmeans.InitVec ./pic_km_in ./pic_km_initvec 10 2879 4

# Usage: Kmeans <in> <out> <init> <# of clusters> <# of nodes> <# reducers>
hadoop jar pic.jar kmeans.Kmeans ./pic_km_in ./pic_km_out ./pic_km_initvec 10 2879 4
rm ../result/pic_km_out.txt
hadoop fs -getmerge ./pic_km_out ../result/pic_km_out.txt
