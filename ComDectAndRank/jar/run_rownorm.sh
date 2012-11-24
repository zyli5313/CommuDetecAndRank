nreducer=$1
inpath=$2

# usage: RowNorm <edge_path> <output_path> <# of reducers> <makesym or nosym>
#hadoop jar rownorm.jar ./pic_input ./pic_rownorm 4 nosys
hadoop jar pic.jar pic.RowNorm $inpath ./pic_rownorm $nreducer nosym
