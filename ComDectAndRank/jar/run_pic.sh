nnode=$1
nreducer=$2
nmaxiter=$3

#usage: Pic <edge_path> <temppic_path> <output_path> <# of nodes>  <# of tasks> <max iteration> <makesym or nosym> <new or contNN>  <start0 or start1>
hadoop jar pic.jar pic.Pic ./pic_rownorm ./pic_tempmv ./pic_iterout $nnode $nreducer $nmaxiter nosym new start1
