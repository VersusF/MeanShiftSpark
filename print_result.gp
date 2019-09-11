set title "Open pubs"
set xlabel "longitude"
set ylabel "latitude"
set datafile separator ","
set object 1 rectangle from screen 0,0 to screen 1,1 fillcolor rgb"white" behind
plot "output/points/part-00000" using 2:1 lt rgb "blue", \
     "output/clusters/part-00000" using 2:1 lt rgb "red" pointtype 1
pause -1 "Hit any key to continue\n"
