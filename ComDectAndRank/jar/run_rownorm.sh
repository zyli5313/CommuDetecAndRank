nreducer=$1

# usage: RowNorm <edge_path> <output_path> <# of reducers> <makesym or nosym>
#hadoop jar rownorm.jar ./pic_input ./pic_rownorm 4 nosys
hadoop jar pic.jar pic.RowNorm ./pic_input/umbcblog_feature.data ./pic_rownorm $nreducer nosys
