package cluster

import "testing"

func TestOwnerStaysUntilThatNodeLeaves(t *testing.T) {
	members := []Member{{ID: "node1"}, {ID: "node2"}, {ID: "node3"}}
	first := HighestRandomWeight("acme", members)
	if got := HighestRandomWeight("acme", members); got.ID != first.ID {
		t.Fatalf("owner changed from %s to %s", first.ID, got.ID)
	}

	remaining := make([]Member, 0, 2)
	for _, member := range members {
		if member.ID != first.ID {
			remaining = append(remaining, member)
		}
	}
	moved := HighestRandomWeight("acme", remaining)
	if moved.ID == first.ID {
		t.Fatalf("customer stayed on departed node %s", first.ID)
	}
	if got := HighestRandomWeight("acme", remaining); got.ID != moved.ID {
		t.Fatalf("new owner changed from %s to %s", moved.ID, got.ID)
	}
}

func TestCustomersSpreadAcrossNodes(t *testing.T) {
	members := []Member{{ID: "node1"}, {ID: "node2"}, {ID: "node3"}}
	seen := map[string]int{}
	for i := 0; i < 30; i++ {
		owner := HighestRandomWeight("customer-"+string(rune('a'+i)), members)
		seen[owner.ID]++
	}
	if len(seen) < 2 {
		t.Fatalf("owners = %v", seen)
	}
}
