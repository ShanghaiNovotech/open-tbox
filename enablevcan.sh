#!/bin/sh
modprobe vcan
ip link add vcan0 type vcan
ip link set vcan0 up
