package cluster

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestHeartbeatAgreesOnOwnerAndLeaveMovesCustomer(t *testing.T) {
	mr := miniredis.RunT(t)
	url := "redis://" + mr.Addr() + "/0"

	node1 := openDir(t, url, Member{ID: "node1", URL: "http://node1:8080"})
	node2 := openDir(t, url, Member{ID: "node2", URL: "http://node2:8080"})
	ctx := context.Background()
	node1.Beat(ctx)
	node2.Beat(ctx)
	node1.Beat(ctx)

	if len(node1.Members()) != 2 || len(node2.Members()) != 2 {
		t.Fatalf("node1 %v node2 %v", node1.Members(), node2.Members())
	}
	if node1.Owner("acme").ID != node2.Owner("acme").ID {
		t.Fatal("nodes disagree on the owner")
	}

	owner := node1.Owner("acme")
	var other *Directory
	if owner.ID == "node1" {
		other = node2
		node1.Leave(ctx)
		node2.Beat(ctx)
	} else {
		other = node1
		node2.Leave(ctx)
		node1.Beat(ctx)
	}
	if got := other.Owner("acme").ID; got == owner.ID {
		t.Fatalf("owner still %s after leave", got)
	}
}

func openDir(t *testing.T, url string, self Member) *Directory {
	t.Helper()
	rdb, err := OpenRedis(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return NewDirectory(self, rdb, nil)
}
