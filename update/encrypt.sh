#!/bin/sh

if [ ! -f "update.tar.xz" ]; then
    echo "Missing update.tar.xz for encrypt."
fi

dd if=tboxhwver bs=1 count=5 > randkey.bin
dd if=tboxfwver bs=1 count=5 >> randkey.bin
openssl rand 118 >> randkey.bin
base64 randkey.bin > key.bin
rm randkey.bin
openssl rsautl -sign -inkey private.pem -in key.bin -out key.bin.enc
openssl enc -aes-256-cbc -salt -in update.tar.xz -out update.tar.xz.enc -pass file:./key.bin
rm key.bin
tar -cf update.tar ./*.enc
rm *.enc
mv update.tar update.dat

