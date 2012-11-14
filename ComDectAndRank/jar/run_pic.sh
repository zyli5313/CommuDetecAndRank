
#usage: Pic <edge_path> <temppic_path> <output_path> <# of nodes>  <# of tasks> <max iteration> <makesym or nosym> <new or contNN>
#hadoop jar pic.jar ./pic_rownorm ./pic_tempmv ./pic_vec 2879 4 30 nosym new
hadoop jar pic.jar pic.Pic ./pic_rownorm ./pic_tempmv ./pic_vec 2879 4 30 nosym new
