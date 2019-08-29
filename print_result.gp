set title "Open pubs"
set xlabel "longitude"
set ylabel "latitude"
set datafile separator ","
plot "output/points/part-00000" using 2:1 lt rgb "blue", \
     "output/clusters/part-00000" using 2:1 lt rgb "red"
pause -1 "Hit any key to continue\n"
