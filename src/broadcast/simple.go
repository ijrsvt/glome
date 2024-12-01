package broadcast

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type simpleBroadcastHandler struct {
	node *maelstrom.Node
	mu   sync.Mutex
	seen map[int]struct{}
}

func (b *simpleBroadcastHandler) handleBroadcast(msg maelstrom.Message) error {
	type broadcast struct {
		Message int
	}
	var body broadcast
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshaling body: %w", err)
	}

	b.mu.Lock()
	b.seen[body.Message] = struct{}{}
	b.mu.Unlock()

	return b.node.Reply(msg, map[string]string{
		"type": "broadcast_ok",
	})
}

func (b *simpleBroadcastHandler) handleRead(msg maelstrom.Message) error {
	resp := map[string]any{
		"type": "read_ok",
	}

	b.mu.Lock()
	values := make([]int, len(b.seen))

	for b := range b.seen {
		values = append(values, b)
	}
	resp["messages"] = values

	b.mu.Unlock()

	return b.node.Reply(msg, resp)
}

func (b *simpleBroadcastHandler) handleTopoloy(msg maelstrom.Message) error {
	return b.node.Reply(msg, map[string]string{
		"type": "topology_ok",
	})
}

func newSimpleHandler(n *maelstrom.Node) *simpleBroadcastHandler {
	b := &simpleBroadcastHandler{
		node: n,
		mu:   sync.Mutex{},
		seen: make(map[int]struct{}),
	}
	return b
}

func RunSimple() {
	node := maelstrom.NewNode()
	h := newSimpleHandler(node)

	node.Handle("broadcast", h.handleBroadcast)
	node.Handle("read", h.handleRead)

	node.Handle("topology", h.handleTopoloy)

	if err := node.Run(); err != nil {
		log.Fatalf("Unable to run: %v", err)
	}
}
