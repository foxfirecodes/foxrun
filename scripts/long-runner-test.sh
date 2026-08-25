#!/bin/bash

id="$(head -c 16 /dev/random | xxd -ps)"
args_hash="$(echo "$@" | md5sum --quiet)"

echo "script id: $id, with args $@ (hash: $args_hash)"

count=1
while true; do
  echo "$id: $((count++)) - $args_hash"
  sleep 1
done
