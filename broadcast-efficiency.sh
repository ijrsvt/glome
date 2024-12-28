go build .
.././maelstrom/maelstrom test -w broadcast --bin ./maelstrom-broadcast.sh --node-count 5 --time-limit 20 --rate 10 --nemesis partition

if [[ $? -ne 0 ]]; then
  echo "the solution is not fault tolerant"
  exit 1
fi

.././maelstrom/maelstrom test -w broadcast --bin ./maelstrom-broadcast.sh --node-count 25 --time-limit 40 --rate 50 --latency 100
