package benzene

import (
	"math/rand"
	"sync"
)

const (
	lKey       = 0
	lVal       = 0
	nNext      = 0
	tMaxHeight = 4
	branching  = 4
)

// 具体每个skip list node里面的数据，应该需要加一个encode和decode的方法来对具体存储做解析
// TODO add encode and decode method
type MutiKV struct {
	keyMap       map[[]byte]int
	valueContent [][]string
}

type ZskipListNode struct {
	// TODO timeKey need to consider more to ensure a struct to storage, maybe []byte
	timeKey       int64
	data          MutiKV
	nextNode      ZskipListNode
	nextLevelNode ZskipListNode
	level         int
}

type MemDB struct {
	// 这里是内存结构的主要部分
	// the lock used to get and put
	mu sync.RWMutex
	// TODO need to add a comparer method
	//cmp comparer.BasicComparer

	// implement a skip list, the node in the skip list is a point to a list, the data in the list is map[byte] []byte. the kvData is the skip list, it is a linked list.
	// all the data are save in this array, through the nodeData to note every K-V information, such as K is 5 byte, v is 6 byte ...
	headNode ZskipListNode

	// 加一个专门的timeKey的dict 用来快速查找
	timeDict map[int64]int
	maxHight int
	rnd      *rand.Rand
}

func (m *MutiKV) decode() {

}

func (m *MutiKV) encode() {

}

func (p *MemDB) randHeight() (h int) {
	h = 1
	for h < tMaxHeight && p.rnd.Int()%branching == 0 {
		h++
	}
	return h
}

func (p *MemDB) IsHasTimeKey(timeStamp int64) {
	_, ok := p.timeDict[timeStamp]
	return ok
}

func (p *MemDB) findTimeKey(timeStamp int64) (ZskipListNode, []ZskipListNode, bool) {
	node := p.headNode
	pre := node
	preList := make([]ZskipListNode, tMaxHeight)
	// 从最顶层的level开始找
	for {
		if node == nil {
			return pre, preList, false
		}
		if node.timeKey == timeStamp {
			// 找到node
			return node, preList, true
		} else if node.timeKey < timeStamp {
			// 继续向后找
			pre = node
			node = node.nextNode
		} else {
			// level往下一层
			preList = append(preList, node)
			pre = node
			node = node.nextLevelNode.nextNode
		}
	}
	return node, preList, false
}

func (p *MemDB) Get(timeKey int64, key byte, value byte) ([]string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	timeNode, _, ok := p.findTimeKey(timeKey)
	if ok {
		if timeNode {
			keyIndex, ok := timeNode.data.keyMap[key]
			if ok {
				return timeNode.data.valueContent[keyIndex], true
			}
		}
	}
	return nil, false
}

func (p *MemDB) Put(timeStamp int64, key byte, value byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	node, preNodeList, ok := p.findTimeKey(timeStamp)
	if !ok {
		p.timeDict[timeStamp] = 1
		// 这次节点的跳表level
		h := p.randHeight() - 1
		// 最底下的那个node
		newNode := &ZskipListNode{
			timeKey: timeStamp,
			data:    &MutiKV{},
			level:   tMaxHeight,
		}

		// 给最底层添加
		tmpNode := node.nextNode
		node.nextNode = newNode
		newNode.nextNode = tmpNode

		// 第h level的节点
		newHNode := &ZskipListNode{
			timeKey: timeStamp,
			// 这里第h层的data和最底层的data是同一个 理论上是一个指针指过去，但是不确定golang
			// 的指针用法有什么坑，需要再细看下确定下
			// TODO consider more about the pointer of Golang
			data:  *newNode.data,
			level: h,
		}

		// 给第h level添加
		tmpHNode := preNodeList[h].nextNode
		preNodeList[h].nextNode = newHNode
		newHNode.nextNode = tmpHNode
		return true
	}

	return false
}

func (p *MemDB) GetRange(start_pos int64, end_pos int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

}

func (p *MemDB) NewDB() MemDB {

	topSkipListNode := &ZskipListNode{
		timeKey: 0,
		data:    MutiKV{},
		level:   tMaxHeight,
	}
	memDB := &MemDB{
		rnd:      rand.New(rand.NewSource(0xdeadbeef)),
		headNode: topSkipListNode,
		timeDict: make(map[int64]int),
		maxHight: tMaxHeight,
	}
	return memDB
}
