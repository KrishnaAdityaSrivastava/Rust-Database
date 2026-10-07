#!/bin/bash

mkdir -p ./data/node1 ./data/node2 ./data/node3

cargo build --bins

xfce4-terminal --title="Raft Node 1" \
  --command="bash -c 'cargo run --bin node -- 1 127.0.0.1:6001 2:127.0.0.1:6002 3:127.0.0.1:6003 --log --dir ./data/node1; exec bash'" &

xfce4-terminal --title="Raft Node 2" \
  --command="bash -c 'cargo run --bin node -- 2 127.0.0.1:6002 1:127.0.0.1:6001 3:127.0.0.1:6003 --log --dir ./data/node2; exec bash'" &

xfce4-terminal --title="Raft Node 3" \
  --command="bash -c 'cargo run --bin node -- 3 127.0.0.1:6003 1:127.0.0.1:6001 2:127.0.0.1:6002 --log --dir ./data/node3; exec bash'" &

sleep 2

xfce4-terminal --title="Raft Client 1" \
  --command="bash -c 'cargo run --bin client -- 127.0.0.1:6002; exec bash'" &

xfce4-terminal --title="Raft Client 2" \
  --command="bash -c 'cargo run --bin client -- 127.0.0.1:6001; exec bash'" &