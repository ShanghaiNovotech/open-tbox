#!/bin/sh

VIN=CE316042500580001
ICCID=89860116963104747820
SERVER=123.57.214.169
PORT=62003

mkdir -p /var/lib/tbox/conf
mkdir -p /var/lib/tbox/log

cp ./tboxparse.xml /var/lib/tbox/conf/

./src/tbox-logger -N $VIN -I $ICCID \
    --fallback-vehicle-server-host=$SERVER \
    --fallback-vehicle-server-port=$PORT \
    --use-vcan
