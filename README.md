# open-tbox 开源-TBOX
tbox = Telematics BOX or T-BOX, it is installed inside a vehicle and connects vehicle's CANBUS/OBD, and GPS, LAN, WIFI, BT & Celluar Connections, providing a gateway for remote CANBUS r/w and other features(controls, internet connections and etc).

The hardware is made by Shanghai Novotech, it is an [NXP i.MX6UL](https://www.nxp.com/products/processors-and-microcontrollers/applications-processors/i.mx-applications-processors/i.mx-6-processors/i.mx-6ultralite-processor-low-power-secure-arm-cortex-a7-core:i.MX6UL) Low power Cortex-A7 industrail/automobile grade processor, 8GB emmc and 256MB DDR3L with 3.6v 800mAh Ni-MH backed UPS power for up to 10 mins emergency power loss backup. 

![tbox PCB IC Side](/pictures/tbox_ic_side.jpg?raw=true "tbox PCB IC Side")

## Software Potocol and Standards
This repo complients with China National GB Standards. 符合中国国标开发的。


## How to compile on Debian/Ubuntu:

### Install necessary packages:

```
sudo apt-get install build-essential autotools-dev libtool autoconf automake libglib2.0-dev libjson-c-dev libgps-dev can-utils
```
### Change working directory into source file directory, run commands below:

```
./autogen.sh
./configure
make
```

## How to run CAN bus emulation on PC:

### Change working directory into source file directory, run commands below:

```
sudo ./enablevcan.sh
./cantestloop.sh
```

### Open another terminal at the same directory, run command below:

```
sudo ./test-tbox-logger-start.sh
```

You can change VIN, ICCID, server address and port in test-tbox-logger-start.sh for testing.


# Help, Contribute and more
Fork it and submit merge request.

yiling.cao[ @ ] shanghainovotech.com for more info.
