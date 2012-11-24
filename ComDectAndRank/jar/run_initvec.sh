nnode=$1
nreducer=$2
inpath=$3

# PicInitVec <edge_path> <output_path> <# of nodes> <# of reducers> <makesym or nosym> <start1 or start0>
hadoop jar pic.jar pic.InitVec $inpath ./pic_out $nnode $nreducer nosym start1
