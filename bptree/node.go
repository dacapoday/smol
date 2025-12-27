package bptree

func writeNodes[
	B ReadWrite,
	V BlockID | []byte,
	Items items[V],
	Item branchItem | leafItem,
	ItemPtr itemPtr[V, Items, Item],
](block B, page *branchPage, head *node[V, Items, Item, ItemPtr]) (err error) {
	writer := nodeWriter[B, V, Items, Item, ItemPtr]{block: block, page: page}
	writer.buffer = block.AllocateBuffer()
	defer block.RecycleBuffer(writer.buffer)

	limit := block.PageSize() - HeadSize
	halfLimit := limit / 2

	var node, rest, next nodes[V, Items, Item, ItemPtr]
	var nodeSize, restSize int
	var noRest bool

	node.node = head
	node.item = &head.page.head
	// node.offset = 0
	// node.count = 0
	for {
		nodeSize, rest, noRest = node.split(limit)
		if noRest {
			if node.count == 0 {
				writer.append(node.prev)
			} else {
				// if restSize < halfLimit {
				// 	// TODO: try merge neighbor
				// }
				err = writer.write(node.prev, HeadSize+nodeSize, node.items)
			}
			if rest.node == nil {
				return
			}
			if err != nil {
				return
			}
			node = rest
			continue
		}

		restSize, next, noRest = rest.split(limit)
		if noRest {
			if restSize < halfLimit {
				// TODO: try merge next
				nodeSize, rest, restSize = node.balance(limit, nodeSize, restSize, rest.count)
			}
			err = writer.write(node.prev, HeadSize+nodeSize, node.items)
			if err != nil {
				return
			}
			err = writer.write(seg{}, HeadSize+restSize, rest.items)
			if next.node == nil {
				return
			}
			if err != nil {
				return
			}
			node = next
			continue
		}

		err = writer.write(node.prev, HeadSize+nodeSize, node.items)
		if err != nil {
			return
		}
		for {
			node = rest
			rest = next
			nodeSize = restSize
			restSize, next, noRest = rest.split(limit)
			if noRest {
				if restSize < halfLimit {
					// TODO: try merge next
					nodeSize, rest, restSize = node.balance(limit, nodeSize, restSize, rest.count)
				}
				err = writer.write(seg{}, HeadSize+nodeSize, node.items)
				if err != nil {
					return
				}
				err = writer.write(seg{}, HeadSize+restSize, rest.items)
				if err != nil {
					return
				}
				break
			}

			err = writer.write(seg{}, HeadSize+nodeSize, node.items)
			if err != nil {
				return
			}
		}
		if next.node == nil {
			return
		}
		node = next
	}
}

// nodeWriter should stack-only; no escape
type nodeWriter[
	B ReadWrite,
	V BlockID | []byte,
	Items items[V],
	Item branchItem | leafItem,
	ItemPtr itemPtr[V, Items, Item],
] struct {
	block B

	page *branchPage
	tail *branchItem

	buffer []byte
}

func (writer *nodeWriter[B, V, Items, Item, ItemPtr]) append(prev seg) {
	item := writer.extend()
	item.prev = prev
}

func (writer *nodeWriter[B, V, Items, Item, ItemPtr]) write(prev seg, size int, items Items) (err error) {
	blockID := writer.block.AllocateBlock()
	if blockID < 2 {
		err = errAllocateFailed(writer.block)
		return
	}

	key := items.encode(writer.buffer[:size])

	err = writer.block.WriteBlock(blockID, writer.buffer)
	if err != nil {
		return
	}

	item := writer.extend()
	item.prev = prev
	item.key = b2s(key)
	item.id = blockID
	return
}

func (writer *nodeWriter[B, V, Items, Item, ItemPtr]) extend() (item *branchItem) {
	if writer.tail == nil {
		item = &writer.page.head
	} else {
		item = writer.tail.extend()
	}
	writer.tail = item
	return
}

type nodes[
	V BlockID | []byte,
	Items items[V],
	Item branchItem | leafItem,
	ItemPtr itemPtr[V, Items, Item],
] struct {
	*node[V, Items, Item, ItemPtr]
	item   ItemPtr
	offset uint16
	count  uint16
}

func (node nodes[V, Items, Item, ItemPtr]) items(yield func([]byte, V) bool) {
	for key, val := range node.item.pageItems(node.Page, node.tail, node.offset) {
		if !yield(key, val) {
			return
		}
		if node.count--; node.count == 0 {
			return
		}
	}
	for key, val := range node.next.items {
		if !yield(key, val) {
			return
		}
		if node.count--; node.count == 0 {
			return
		}
	}
}

func (node *nodes[V, Items, Item, ItemPtr]) split(limit int) (nodeSize int, rest nodes[V, Items, Item, ItemPtr], noRest bool) {
	// node.count = 0
	rest.node = node.node
	page := rest.Page
	tail := rest.page.tail
	rest.item = node.item
	rest.offset = node.offset
	var pageSize int
	var pageCount uint16
	for {
		pageSize, pageCount, noRest, rest.offset, rest.item = rest.item.split(limit, page, tail, rest.offset)
		nodeSize += pageSize
		node.count += pageCount
		if !noRest {
			return
		}
		if rest.next == nil {
			rest.node = nil
			rest.item = nil
			rest.offset = 0
			return
		}
		if rest.next.prev.beg != rest.next.prev.end {
			rest.node = rest.next
			rest.item = &rest.page.head
			rest.offset = 0
			return
		}
		rest.node = rest.next
		page = rest.Page
		tail = rest.page.tail
		rest.item = &rest.page.head
		rest.offset = 0
		limit -= pageSize
	}
}

func (node *nodes[V, Items, Item, ItemPtr]) balance(limit, size, nextSize int, nextCount uint16) (nodeSize int, rest nodes[V, Items, Item, ItemPtr], restSize int) {
	rest.node = node.node
	page := rest.Page
	tail := rest.page.tail
	rest.item = node.item
	rest.offset = node.offset
	var pageSize int
	var pageCount, count uint16
	var noRest bool
	threshold := size - (limit - nextSize)
	for {
		pageSize, pageCount, noRest, rest.offset, rest.item = rest.item.split(threshold, page, tail, rest.offset)
		nodeSize += pageSize
		count += pageCount
		if !noRest {
			break
		}
		rest.node = rest.next
		page = rest.Page
		tail = rest.page.tail
		rest.item = &rest.page.head
		rest.offset = 0
		threshold -= pageSize
	}
	half := (node.count-count)/2 + 1
	for {
		pageSize, pageCount, noRest, rest.offset, rest.item = rest.item.take(half, page, tail, rest.offset)
		nodeSize += pageSize
		count += pageCount
		if !noRest {
			break
		}
		rest.node = rest.next
		page = rest.Page
		tail = rest.page.tail
		rest.item = &rest.page.head
		rest.offset = 0
		half -= pageCount
	}

	rest.count = nextCount + (node.count - count)
	restSize = nextSize + (size - nodeSize)
	node.count = count
	return
}

type list[
	V BlockID | []byte,
	Items items[V],
	Item branchItem | leafItem,
	ItemPtr itemPtr[V, Items, Item],
] struct {
	head, tail *node[V, Items, Item, ItemPtr]
}

func (list *list[V, Items, Item, ItemPtr]) trunk() bool {
	if list.head == nil || list.tail == nil {
		return false
	}
	if !list.head.level.first() {
		return false
	}
	if !list.tail.level.last() {
		return false
	}
	prev := list.head
	node := prev.next
	for node != nil {
		if !prev.level.nextTo(node.level) {
			return false
		}
		prev = node
		node = prev.next
	}
	return true
}

func (list *list[V, Items, Item, ItemPtr]) extend() (tail *node[V, Items, Item, ItemPtr]) {
	tail = new(node[V, Items, Item, ItemPtr])
	if list.tail == nil {
		list.head = tail
	} else {
		list.tail.next = tail
	}
	list.tail = tail
	return
}

type leafNode = node[[]byte, LeafItems, leafItem, *leafItem]
type branchNode = node[BlockID, BranchItems, branchItem, *branchItem]

type node[
	V BlockID | []byte,
	Items items[V],
	Item branchItem | leafItem,
	ItemPtr itemPtr[V, Items, Item],
] struct {
	next  *node[V, Items, Item, ItemPtr]
	level Level
	page[V, Items, Item, ItemPtr]
	prev seg
}

func (node *node[V, Items, Item, ItemPtr]) items(yield func([]byte, V) bool) {
	for ; node != nil; node = node.next {
		for key, val := range node.page.items {
			if !yield(key, val) {
				return
			}
		}
	}
}

// type leafPage = page[[]byte, LeafItems, leafItem, *leafItem]
type branchPage = page[BlockID, BranchItems, branchItem, *branchItem]

type page[
	V BlockID | []byte,
	Items items[V],
	Item branchItem | leafItem,
	ItemPtr itemPtr[V, Items, Item],
] struct {
	Page
	head Item
	tail seg
}

func (page *page[V, Items, Item, ItemPtr]) items(yield func([]byte, V) bool) {
	for key, val := range ItemPtr(&page.head).pageItems(page.Page, page.tail, 0) {
		if !yield(key, val) {
			return
		}
	}
}

type itemPtr[
	V BlockID | []byte,
	Items items[V],
	Item branchItem | leafItem,
] interface {
	*Item
	pageItems(page Page, tail seg, offset uint16) Items
	split(limit int, page Page, tail seg, offset uint16) (size int, count uint16, noRest bool, restOffset uint16, restItem *Item)
	take(n uint16, page Page, tail seg, offset uint16) (size int, count uint16, noRest bool, restOffset uint16, restItem *Item)
}

type leafItem struct {
	next    *leafItem
	key     string
	val     string
	prev    seg
	defined bool
}

func (item *leafItem) has() bool {
	return item.defined
}

func (item *leafItem) extend() *leafItem {
	item.next = new(leafItem)
	return item.next
}

func (item *leafItem) pageItems(page Page, tail seg, offset uint16) LeafItems {
	return func(yield func([]byte, []byte) bool) {
		if item == nil {
			for key, val := range page.LeafItems(tail.beg+offset, tail.end) {
				if !yield(key, val) {
					return
				}
			}
			return
		}

		for key, val := range page.LeafItems(item.prev.beg+offset, item.prev.end) {
			if !yield(key, val) {
				return
			}
		}
		if item.has() {
			if !yield(s2b(item.key), s2b(item.val)) {
				return
			}
		}
		for item = item.next; item != nil; item = item.next {
			for key, val := range page.LeafItems(item.prev.beg, item.prev.end) {
				if !yield(key, val) {
					return
				}
			}
			if item.has() {
				if !yield(s2b(item.key), s2b(item.val)) {
					return
				}
			}
		}
		for key, val := range page.LeafItems(tail.beg, tail.end) {
			if !yield(key, val) {
				return
			}
		}
	}
}

func (item *leafItem) split(limit int, page Page, tail seg, offset uint16) (size int, count uint16, noRest bool, restOffset uint16, restItem *leafItem) {
	restOffset = offset
	if item == nil {
		for itemSize := range page.LeafItems(tail.beg+offset, tail.end).ItemSize {
			size += itemSize
			if size > limit {
				size -= itemSize
				return
			}
			count++
			restOffset++
		}
		noRest = true
		return
	}
	restItem = item
	for itemSize := range page.LeafItems(item.prev.beg+offset, item.prev.end).ItemSize {
		size += itemSize
		if size > limit {
			size -= itemSize
			return
		}
		count++
		restOffset++
	}
	if item.has() {
		itemSize := leafItemSize(len(item.key), len(item.val))
		size += itemSize
		if size > limit {
			size -= itemSize
			return
		}
		count++
	}
	for item = item.next; item != nil; item = item.next {
		restOffset = 0
		restItem = item
		for itemSize := range page.LeafItems(item.prev.beg, item.prev.end).ItemSize {
			size += itemSize
			if size > limit {
				size -= itemSize
				return
			}
			count++
			restOffset++
		}
		if item.has() {
			itemSize := leafItemSize(len(item.key), len(item.val))
			size += itemSize
			if size > limit {
				size -= itemSize
				return
			}
			count++
		}
	}
	restOffset = 0
	restItem = nil
	for itemSize := range page.LeafItems(tail.beg, tail.end).ItemSize {
		size += itemSize
		if size > limit {
			size -= itemSize
			return
		}
		count++
		restOffset++
	}
	noRest = true
	return
}

func (item *leafItem) take(n uint16, page Page, tail seg, offset uint16) (size int, count uint16, noRest bool, restOffset uint16, restItem *leafItem) {
	restOffset = offset
	if item == nil {
		for itemSize := range page.LeafItems(tail.beg+offset, tail.end).ItemSize {
			if count >= n {
				return
			}
			size += itemSize
			count++
			restOffset++
		}
		noRest = true
		return
	}
	restItem = item
	for itemSize := range page.LeafItems(item.prev.beg+offset, item.prev.end).ItemSize {
		if count >= n {
			return
		}
		size += itemSize
		count++
		restOffset++
	}
	if item.has() {
		if count >= n {
			return
		}
		itemSize := leafItemSize(len(item.key), len(item.val))
		size += itemSize
		count++
	}
	for item = item.next; item != nil; item = item.next {
		restOffset = 0
		restItem = item
		for itemSize := range page.LeafItems(item.prev.beg, item.prev.end).ItemSize {
			if count >= n {
				return
			}
			size += itemSize
			count++
			restOffset++
		}
		if item.has() {
			if count >= n {
				return
			}
			itemSize := leafItemSize(len(item.key), len(item.val))
			size += itemSize
			count++
		}
	}
	restOffset = 0
	restItem = nil
	for itemSize := range page.LeafItems(tail.beg, tail.end).ItemSize {
		if count >= n {
			return
		}
		size += itemSize
		count++
		restOffset++
	}
	noRest = true
	return
}

type branchItem struct {
	next *branchItem
	key  string
	id   BlockID
	prev seg
}

func (item *branchItem) has() bool {
	return item.id > 1
}

func (item *branchItem) extend() *branchItem {
	item.next = new(branchItem)
	return item.next
}

func (item *branchItem) pageItems(page Page, tail seg, offset uint16) BranchItems {
	return func(yield func([]byte, BlockID) bool) {
		if item == nil {
			for key, id := range page.BranchItems(tail.beg+offset, tail.end) {
				if !yield(key, id) {
					return
				}
			}
			return
		}

		for key, id := range page.BranchItems(item.prev.beg+offset, item.prev.end) {
			if !yield(key, id) {
				return
			}
		}
		if item.has() {
			if !yield(s2b(item.key), item.id) {
				return
			}
		}
		for item = item.next; item != nil; item = item.next {
			for key, id := range page.BranchItems(item.prev.beg, item.prev.end) {
				if !yield(key, id) {
					return
				}
			}
			if item.has() {
				if !yield(s2b(item.key), item.id) {
					return
				}
			}
		}
		for key, id := range page.BranchItems(tail.beg, tail.end) {
			if !yield(key, id) {
				return
			}
		}
	}
}

func (item *branchItem) split(limit int, page Page, tail seg, offset uint16) (size int, count uint16, noRest bool, restOffset uint16, restItem *branchItem) {
	restOffset = offset
	if item == nil {
		for itemSize := range page.BranchItems(tail.beg+offset, tail.end).ItemSize {
			size += itemSize
			if size > limit {
				size -= itemSize
				return
			}
			count++
			restOffset++
		}
		noRest = true
		return
	}
	restItem = item
	for itemSize := range page.BranchItems(item.prev.beg+offset, item.prev.end).ItemSize {
		size += itemSize
		if size > limit {
			size -= itemSize
			return
		}
		count++
		restOffset++
	}
	if item.has() {
		itemSize := branchItemSize(len(item.key))
		size += itemSize
		if size > limit {
			size -= itemSize
			return
		}
		count++
	}
	for item = item.next; item != nil; item = item.next {
		restOffset = 0
		restItem = item
		for itemSize := range page.BranchItems(item.prev.beg, item.prev.end).ItemSize {
			size += itemSize
			if size > limit {
				size -= itemSize
				return
			}
			count++
			restOffset++
		}
		if item.has() {
			itemSize := branchItemSize(len(item.key))
			size += itemSize
			if size > limit {
				size -= itemSize
				return
			}
			count++
		}
	}
	restOffset = 0
	restItem = nil
	for itemSize := range page.BranchItems(tail.beg, tail.end).ItemSize {
		size += itemSize
		if size > limit {
			size -= itemSize
			return
		}
		count++
		restOffset++
	}
	noRest = true
	return
}

func (item *branchItem) take(n uint16, page Page, tail seg, offset uint16) (size int, count uint16, noRest bool, restOffset uint16, restItem *branchItem) {
	restOffset = offset
	if item == nil {
		for itemSize := range page.BranchItems(tail.beg+offset, tail.end).ItemSize {
			if count >= n {
				return
			}
			size += itemSize
			count++
			restOffset++
		}
		noRest = true
		return
	}
	restItem = item
	for itemSize := range page.BranchItems(item.prev.beg+offset, item.prev.end).ItemSize {
		if count >= n {
			return
		}
		size += itemSize
		count++
		restOffset++
	}
	if item.has() {
		if count >= n {
			return
		}
		itemSize := branchItemSize(len(item.key))
		size += itemSize
		count++
	}
	for item = item.next; item != nil; item = item.next {
		restOffset = 0
		restItem = item
		for itemSize := range page.BranchItems(item.prev.beg, item.prev.end).ItemSize {
			if count >= n {
				return
			}
			size += itemSize
			count++
			restOffset++
		}
		if item.has() {
			if count >= n {
				return
			}
			itemSize := branchItemSize(len(item.key))
			size += itemSize
			count++
		}
	}
	restOffset = 0
	restItem = nil
	for itemSize := range page.BranchItems(tail.beg, tail.end).ItemSize {
		if count >= n {
			return
		}
		size += itemSize
		count++
		restOffset++
	}
	noRest = true
	return
}

func (item *branchItem) items(yield func([]byte, BlockID) bool) {
	for ; item != nil; item = item.next {
		if !yield(s2b(item.key), item.id) {
			return
		}
	}
}

type seg = struct{ beg, end uint16 }
