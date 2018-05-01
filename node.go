package hashmap

type node struct {
	next *node
	key string
	value string
}

func (n *node) append(nn *node) {
	for n.next != nil {
		n = n.next
	}
	n.next = nn
}

