#!/bin/python
# validate if output is sorted

import os

def issame(fpwith, fpwithout):
    fw = file(fpwith, 'r')
    fwo = file(fpwithout, 'r')

    lw = fw.readline()
    lwo = fwo.readline()
    while len(lw)!=0 and len(lwo)!=0:
        listw = lw.split('\t')
        if listw[1] != lwo:
            return False
        lw = fw.readline()
        lwo = fwo.readline()

	if len(lw)!=0 and len(lwo)==0:
		return False
    if len(lwo)!=0 and len(lw)==0:
    	return False
	return True

def main():
    fpw = './pic_km_outkey.txt'
    fpwo = './pic_km_out.txt'

    if issame(fpw, fpwo):
        print('not same\n')
    else:
        print('same\n')

main()
