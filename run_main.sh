#!/bin/bash

# Working with IPtables
use_IPtables=true

if [ "$use_IPtables" = true ] ; then
    sysctl -w net.ipv4.ip_forward=1
    iptables -t nat -A OUTPUT -d 172.18.0.2 -j DNAT --to-destination 10.100.16.60
fi

# Activate conda-environment(py36m), 
# and execute PDFExtractor(main.py)
source activate py36m
python Docker/test.py