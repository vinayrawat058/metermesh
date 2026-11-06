package cluster

import "hash/fnv"

// Member is one live MeterMesh node.
type Member struct {
	ID  string
	URL string
}

// HighestRandomWeight picks the owner of a customer. The same customer stays
// on the same member until that member disappears, and only that customer's
// events move when a node leaves.
func HighestRandomWeight(customerID string, members []Member) Member {
	var best Member
	var bestScore uint64
	for i, member := range members {
		score := weight(customerID, member.ID)
		if i == 0 || score > bestScore || (score == bestScore && member.ID < best.ID) {
			best = member
			bestScore = score
		}
	}
	return best
}

func weight(customerID, nodeID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(customerID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(nodeID))
	return h.Sum64()
}
