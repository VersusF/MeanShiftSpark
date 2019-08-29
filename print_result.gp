set title "Open pubs"
set xlabel "longitude"
set ylabel "latitude"
set datafile separator ","
plot "points/part-00000" using 2:1 lt rgb "blue", \
     "clusters/part-00000" using 2:1 lt rgb "red"
pause -1 "Hit any key to continue\n"
