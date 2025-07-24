#!/bin/sh

for fl in *.csv
do
  iconv -f cp1251 -t utf8 "$fl" > "u_$fl"
done