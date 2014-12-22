#!/bin/bash
for i in {1..5}
do
    sleep 2s
    echo "Welcome $i times"
    touch "ddd$i"
done
