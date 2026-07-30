#!/bin/bash
pkill -f 'python3.*cyberscore_try'
sleep 2
cd /root/main
bash run_dltv2.sh > /tmp/dltv_restart.log 2>&1 &
sleep 1
ps aux | grep cyberscore_try | grep -v grep