package broadcast

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastHandler struct {
	node *maelstrom.Node
	mu   sync.Mutex
	seen sync.Map
}

func (b *BroadcastHandler) handleShare(neighbor string, body json.RawMessage) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	if _, err := b.node.SyncRPC(ctx, neighbor, body); err != nil {
		log.Printf("Failed to send to neighbor: %s\n", neighbor)
		b.handleShare(neighbor, body)
	}
	cancel()
}

func (b *BroadcastHandler) handleBroadcast(msg maelstrom.Message) error {
	type broadcast struct {
		Message int
	}
	var body broadcast
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("unmarshaling body: %w", err)
	}

	_, exists := b.seen.LoadOrStore(body.Message, struct{}{})

	isNode := slices.Contains(b.node.NodeIDs(), msg.Src)

	if !exists && !isNode {
		neighbors := b.node.NodeIDs()
		for _, n := range neighbors {
			if n != msg.Src && n != b.node.ID() {
				go b.handleShare(n, msg.Body)
			}
		}
	}

	return b.node.Reply(msg, map[string]string{
		"type": "broadcast_ok",
	})
}

func (b *BroadcastHandler) handleRead(msg maelstrom.Message) error {
	resp := map[string]any{
		"type": "read_ok",
	}

	var values []int

	b.seen.Range(func(key, _ any) bool {
		values = append(values, (key).(int))
		return true
	})

	resp["messages"] = values
	return b.node.Reply(msg, resp)
}

func (b *BroadcastHandler) handleTopoloy(msg maelstrom.Message) error {
	return b.node.Reply(msg, map[string]string{
		"type": "topology_ok",
	})
}

func NewHandler(n *maelstrom.Node) *BroadcastHandler {
	b := &BroadcastHandler{
		node: n,
		mu:   sync.Mutex{},
		seen: sync.Map{},
	}
	return b
}

func RunMulti() {
	node := maelstrom.NewNode()
	h := NewHandler(node)

	node.Handle("broadcast", h.handleBroadcast)
	node.Handle("read", h.handleRead)

	node.Handle("topology", h.handleTopoloy)

	if err := node.Run(); err != nil {
		log.Fatalf("Unable to run: %v", err)
	}
}
