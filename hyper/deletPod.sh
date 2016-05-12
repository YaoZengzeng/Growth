#! /bin/bash

for i in $( echo '940512' | sudo ./hyper list | awk 'NR>2 {print $1}') 
do
sudo ./hyper rm $i
done
