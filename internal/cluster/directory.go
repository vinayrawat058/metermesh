package cluster

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	nodeKeyPrefix     = "metermesh:node:"
	membershipTTL     = 3 * time.Second
	heartbeatInterval = time.Second
)

// Directory is the set of nodes currently heartbeating through Redis.
// A nil Redis client means this process is the only member.
type Directory struct {
	self Member
	rdb  *redis.Client
	log  *slog.Logger

	mu      sync.Mutex
	members []Member
}

// OpenRedis connects to the membership store.
func OpenRedis(url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	rdb := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	return rdb, nil
}

// NewDirectory returns a directory that initially contains only this node.
func NewDirectory(self Member, rdb *redis.Client, log *slog.Logger) *Directory {
	if log == nil {
		log = slog.Default()
	}
	return &Directory{
		self:    self,
		rdb:     rdb,
		log:     log,
		members: []Member{self},
	}
}

// Owner returns the node that should total this customer.
func (d *Directory) Owner(customerID string) Member {
	members := d.Members()
	if len(members) == 0 {
		return d.self
	}
	return HighestRandomWeight(customerID, members)
}

// Members returns a copy of the live nodes.
func (d *Directory) Members() []Member {
	d.lock()
	defer d.unlock()
	out := make([]Member, len(d.members))
	copy(out, d.members)
	return out
}

// ReplaceMembers sets the live set. An empty set falls back to this node.
func (d *Directory) ReplaceMembers(members []Member) {
	if len(members) == 0 {
		members = []Member{d.self}
	}
	members = append([]Member(nil), members...)
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })

	d.lock()
	changed := !sameMembers(d.members, members)
	d.members = members
	d.unlock()

	if changed {
		d.log.Info("membership updated", "members", memberIDs(members))
	}
}

// Run refreshes this node's heartbeat until ctx is canceled.
// It does not remove the Redis key. Call Leave after the final flush so
// customers move only once the last totals are saved.
func (d *Directory) Run(ctx context.Context) {
	d.Beat(ctx)
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.Beat(context.Background())
		}
	}
}

// Beat publishes this node and reloads the live set.
func (d *Directory) Beat(ctx context.Context) {
	if d.rdb == nil {
		return
	}
	if err := d.rdb.Set(ctx, nodeKeyPrefix+d.self.ID, d.self.URL, membershipTTL).Err(); err != nil {
		d.log.Warn("heartbeat failed", "error", err)
		return
	}
	members, err := d.load(ctx)
	if err != nil {
		d.log.Warn("membership load failed", "error", err)
		return
	}
	d.ReplaceMembers(ensureSelf(members, d.self))
}

// Leave removes this node from the live set so survivors can take its customers
// without waiting for the heartbeat TTL.
func (d *Directory) Leave(ctx context.Context) {
	if d.rdb == nil {
		return
	}
	if err := d.rdb.Del(ctx, nodeKeyPrefix+d.self.ID).Err(); err != nil {
		d.log.Warn("leave failed", "error", err)
		return
	}
	d.log.Info("left membership", "node_id", d.self.ID)
}

// MembershipTTL is how long a missing heartbeat can sit before survivors drop the node.
func MembershipTTL() time.Duration {
	return membershipTTL
}

func (d *Directory) load(ctx context.Context) ([]Member, error) {
	var (
		cursor uint64
		keys   []string
	)
	for {
		found, next, err := d.rdb.Scan(ctx, cursor, nodeKeyPrefix+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, found...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}
	values, err := d.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	members := make([]Member, 0, len(keys))
	for i, key := range keys {
		url, ok := values[i].(string)
		if !ok || url == "" {
			continue
		}
		members = append(members, Member{
			ID:  strings.TrimPrefix(key, nodeKeyPrefix),
			URL: url,
		})
	}
	return members, nil
}

func ensureSelf(members []Member, self Member) []Member {
	for _, member := range members {
		if member.ID == self.ID {
			return members
		}
	}
	return append(members, self)
}

func sameMembers(a, b []Member) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].URL != b[i].URL {
			return false
		}
	}
	return true
}

func memberIDs(members []Member) []string {
	ids := make([]string, len(members))
	for i, member := range members {
		ids[i] = member.ID
	}
	return ids
}

func (d *Directory) lock()   { d.mu.Lock() }
func (d *Directory) unlock() { d.mu.Unlock() }
