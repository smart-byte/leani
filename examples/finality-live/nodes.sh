#!/bin/sh
# One node per chain, side by side on this host: Ethereum's runs the blobs
# processor, every rollup's block-summary.
#
#   nodes.sh init  DIR   write DIR/<chain>/leani.toml for every chain
#   nodes.sh start DIR   start every node, logging to DIR/<chain>/node.log
#   nodes.sh stop  DIR   stop them
#   nodes.sh hub   DIR   print the hub's --chain and --ethereum-rpc arguments
#
# LEANI names the binary (default: leani), CHAINS the chain entries.
set -eu

LEANI=${LEANI:-leani}
CHAINS=${CHAINS:-"ethereum-mainnet op-mainnet base-mainnet arbitrum-one robinhood-mainnet"}
command=${1:?usage: nodes.sh init|start|stop|hub DIR}
root=${2:?usage: nodes.sh init|start|stop|hub DIR}

index=0
for chain in $CHAINS; do
  dir="$root/$chain"
  api=$((18100 + index))
  case $command in
    init)
      mkdir -p "$dir"
      # A memory limit makes init write the full schema, whose ports a
      # starter configuration does not expose.
      (cd "$dir" && "$LEANI" init blocks --network "$chain" --memory 1GiB --yes --config "$dir/leani.toml" >"$dir/init.log")
      # Ports of their own: the API, JSON-RPC, and an op-stack chain's gossip.
      sed -i.bak \
        -e "s/^bind = \"127.0.0.1:18080\"/bind = \"127.0.0.1:$api\"/" \
        -e "s/^http_bind = \"127.0.0.1:18545\"/http_bind = \"127.0.0.1:$((18200 + index))\"/" \
        -e "s/^ws_bind = \"127.0.0.1:18546\"/ws_bind = \"127.0.0.1:$((18300 + index))\"/" \
        -e "s/^discovery_port = 9222/discovery_port = $((9222 + index))/" \
        -e "s/^listen_port = 9222/listen_port = $((9222 + index))/" \
        "$dir/leani.toml"
      if [ "$chain" = ethereum-mainnet ]; then
        # Every blob transaction, with its sender and the address it posts to.
        sed -i.bak \
          -e 's/^id = "block-summary"/id = "blobs-money"/' \
          -e 's/^instance = "block-summary-[0-9-]*"/instance = "blobs-money-live"/' \
          -e 's/^version = "1.2.0"/version = "1.6.0"/' \
          -e 's/^start_block = 0$/start_block = 19426589/' \
          "$dir/leani.toml"
      fi
      rm -f "$dir/leani.toml.bak"
      ;;
    start)
      (cd "$dir" || exit 1
        nohup "$LEANI" --config "$dir/leani.toml" --log-format json --log-filter info serve >"$dir/node.log" 2>&1 &
        echo $! >"$dir/node.pid")
      ;;
    stop)
      [ -f "$dir/node.pid" ] && kill "$(cat "$dir/node.pid")" 2>/dev/null || true
      rm -f "$dir/node.pid"
      ;;
    hub)
      printf -- '--chain %s=http://127.0.0.1:%s ' "$chain" "$api"
      # Its JSON-RPC serves the Nitro batch posts the hub reads.
      if [ "$chain" = ethereum-mainnet ]; then
        printf -- '--ethereum-rpc http://127.0.0.1:%s ' "$((18200 + index))"
      fi
      ;;
  esac
  index=$((index + 1))
done
[ "$command" = hub ] && echo
exit 0
