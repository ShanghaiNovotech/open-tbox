## open-tbox



## How to compile on Debian/Ubuntu:

### Install necessary packages:

```sudo apt-get install build-essential autotools-dev libtool autoconf automake libglib2.0-dev libjson-c-dev libgps-dev can-utils
```
### Change working directory into source file directory, run commands below:

```./autogen.sh
./configure
make
```

## How to run CAN bus emulation on PC:

### Change working directory into source file directory, run commands below:

```sudo ./enablevcan.sh
./cantestloop.sh
```

### Open another terminal at the same directory, run command below:

```sudo ./test-tbox-logger-start.sh
```

You can change VIN, ICCID, server address and port in test-tbox-logger-start.sh for testing.
