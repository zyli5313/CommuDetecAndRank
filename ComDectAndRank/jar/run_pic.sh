nnode=$0
nreducer=$1
nmaxiter=$2

#usage: Pic <edge_path> <temppic_path> <output_path> <# of nodes>  <# of tasks> <max iteration> <makesym or nosym> <new or contNN>
hadoop jar pic.jar pic.Pic ./pic_rownorm ./pic_tempmv ./pic_iterout $nnode $nreducer $nmaxiter nosym new
