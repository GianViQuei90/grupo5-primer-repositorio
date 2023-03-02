#!/bin/bash
i=0

BASEDIR=$(dirname $0)

cd $BASEDIR

while true
do
   echo "$i"
   let i++

   python3 sqs_event_handler.py 

   sleep 5
done