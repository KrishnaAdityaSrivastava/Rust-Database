
#!/bin/bash
set -e

# Usage: ./run.sh <servers> <clients>
if [[ $# -ne 2 || ! "$1" =~ ^[1-9][0-9]*$ || ! "$2" =~ ^[0-9]+$ ]]; then
    echo "Usage: $0 <number_of_servers> <number_of_clients>"
    echo "Example: $0 5 3"
    exit 1
fi

SERVERS=$1
CLIENTS=$2

# Base ports
RAFT_PORT=6001
API_PORT=7001
GRPC_PORT=8001

mkdir -p ./data

echo "Building binaries..."
cargo build --bins

# Start Raft servers
for ((i = 1; i <= SERVERS; i++)); do
    raft_port=$((RAFT_PORT + i - 1))
    api_port=$((API_PORT + i - 1))
    grpc_port=$((GRPC_PORT + i - 1))
    data_dir="./data/node$i"

    mkdir -p "$data_dir"

    # Generate peer arguments for every other server
    peers=""
    for ((j = 1; j <= SERVERS; j++)); do
        if ((j != i)); then
            peer_port=$((RAFT_PORT + j - 1))
            peers+=" $j:127.0.0.1:$peer_port"
        fi
    done

    echo "Starting Raft Node $i..."

    xfce4-terminal \
        --title="Raft Node $i" \
        --command="bash -c 'cargo run --bin node -- $i 127.0.0.1:$raft_port$peers --api-addr 127.0.0.1:$api_port --grpc-addr 127.0.0.1:$grpc_port --log --dir $data_dir; exec bash'" &
done

# Start clients
for ((i = 1; i <= CLIENTS; i++)); do
    echo "Starting Raft Client $i..."

    xfce4-terminal \
        --title="Raft Client $i" \
        --command="bash -c 'cargo run --bin client -- 127.0.0.1:$RAFT_PORT; exec bash'" &
done

echo "Started $SERVERS server(s) and $CLIENTS client(s)."
