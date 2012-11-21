nnode=$1
nreducer=$2

# PicInitVec <edge_path> <output_path> <# of nodes> <# of reducers> <makesym or nosym> <start1 or start0>
hadoop jar pic.jar pic.InitVec ./pic_input/umbcblog_feature.data ./pic_out $nnode $nreducer nosym start1
