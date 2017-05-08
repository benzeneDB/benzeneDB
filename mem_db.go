package benzene

import (
	"errors"
	"math/rand"
	"sync/atomic"
	"time"
	"unsafe"
	"sync"
)

const tMaxHeight = 12

type mNode struct {
	timeKey int64
	value   map[string]float64
	next    []unsafe.Pointer
}

func newNode(timeKey int64, value map[string]float64, height int32) *mNode {
	return &mNode{timeKey, value, make([]unsafe.Pointer, height)}
}

func (p *mNode) getNext(n int) *mNode {
	return (*mNode)(atomic.LoadPointer(&p.next[n]))
}

func (p *mNode) setNext(n int, x *mNode) {
	atomic.StorePointer(&p.next[n], unsafe.Pointer(x))
}

func (p *mNode) getNext_NB(n int) *mNode {
	return (*mNode)(p.next[n])
}

func (p *mNode) setNext_NB(n int, x *mNode) {
	p.next[n] = unsafe.Pointer(x)
}

// DB represent an in-memory key/value database.
type DB struct {
	rnd       *rand.Rand
	mu        sync.RWMutex
	head      *mNode
	maxHeight int32
	kvSize    int64
	n         int32
	prev      [tMaxHeight]*mNode
}

// New create new initalized in-memory key/value database.
func NewDB() *DB {
	return &DB{
		rnd:       rand.New(rand.NewSource(0xdeadbeef)),
		maxHeight: 1,
		head:      newNode(time.Now().Unix(), make(map[string]float64), tMaxHeight),
	}
}

func (p *DB) Put(timeKey int64, key string, value float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if m, exact := p.findGE_NB(timeKey, true); exact {
		//h := int32(len(m.next))
		m.value[key] = value
		//x := newNode(key, value, h)
		//for i, n := range p.prev[:h] {
		//	m.setNext_NB(i, m.getNext_NB(i))
		//	n.setNext(i, m)
		//}
		p.kvSize += int64(len(key)) + int64(value)
		//atomic.AddInt64(&p.kvSize, int64(len(key))+int64(value))
		return
	}

	h := p.randHeight()
	if h > p.maxHeight {
		for i := p.maxHeight; i < h; i++ {
			p.prev[i] = p.head
		}
		p.maxHeight = h
		//atomic.StoreInt32(&p.maxHeight, h)
	}
	v := make(map[string]float64)
	v[key] = value
	x := newNode(timeKey, v, h)
	for i, n := range p.prev[:h] {
		x.setNext_NB(i, n.getNext_NB(i))
		n.setNext(i, x)
	}

	p.kvSize += int64(len(key)) + int64(value)
	p.n += 1
	//atomic.AddInt64(&p.kvSize, int64(len(key))+int64(value))
	//atomic.AddInt32(&p.n, 1)
}

func (p *DB) Remove(key int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	x, exact := p.findGE_NB(key, true)
	if !exact {
		return
	}

	h := len(x.next)
	for i, n := range p.prev[:h] {
		n.setNext(i, n.getNext_NB(i).getNext_NB(i))
	}

	// TODO
	//p.kvSize -=
	//atomic.AddInt64(&p.kvSize, -int64(x.timeKey + len(x.value)))
	//atomic.AddInt32(&p.n, -1)
}

func (p *DB) Contains(timeKey int64) bool {
	_, exact := p.findGE(timeKey, false)
	return exact
}

func (p *DB) Get(timeKey int64, key string) (float64, error) {
	if x, exact := p.findGE(timeKey, false); exact {
		value, ok := x.value[key]
		if ok {
			return value, nil
		}
		//return x.value, nil
	}
	return -1.0, errors.New("")
}

func (p *DB) GetRange(startTime int64, endTime int64, key string) ([]int64, []float64, error) {
	//var res []float64
	resKey, resValue, ok := p.findRange(startTime, endTime, key)
	if ok {
		return resKey, resValue, nil
	}
	return resKey, resValue, nil
}

func (p *DB) Find(key int64) (int64, map[string]float64, error) {
	if x, _ := p.findGE(key, false); x != nil {
		return x.timeKey, x.value, nil
	}
	return -1, nil, errors.New("")
}

// NewIterator create a new iterator over the database content.
func (p *DB) NewIterator() *Iterator {
	return &Iterator{p: p}
}

// Size return sum of key/value size.
func (p *DB) Size() int {
	return int(atomic.LoadInt64(&p.kvSize))
}

// Len return the number of entries in the database.
func (p *DB) Len() int {
	return int(atomic.LoadInt32(&p.n))
}

// Must hold RW-lock if prev == true, as it use shared prevNode slice.
func (p *DB) findGE(key int64, prev bool) (*mNode, bool) {
	x := p.head
	h := int(atomic.LoadInt32(&p.maxHeight)) - 1
	for {
		next := x.getNext(h)
		var cmp int64
		cmp = 1
		if next != nil {
			cmp = key - next.timeKey
		}
		if cmp < 0 {
			// Keep searching in this list
			x = next
		} else {
			if prev {
				p.prev[h] = x
			} else if cmp == 0 {
				return next, true
			}
			if h == 0 {
				return next, cmp == 0
			}
			h--
		}
	}
	return nil, false
}

// Must hold RW-lock if prev == true, as it use shared prevNode slice.
func (p *DB) findRange(startKey int64, endKey int64, key string) ([]int64, []float64, bool) {
	x := p.head
	h := 0
	var resValue []float64
	var resKey [] int64
	for {
		next := x.getNext(h)
		if next == nil {
			return resKey, resValue, true
		} else {
			if startKey > next.timeKey {
				return resKey, resValue, true
			} else if endKey < next.timeKey {
				x = next
			} else if startKey <= next.timeKey && next.timeKey <= endKey {
				// Keep searching in this list
				resValue = append(resValue, next.value[key])
				resKey = append(resKey, next.timeKey)
				x = next
			}
		}
	}
	return resKey, resValue, false
}



// Must hold RW-lock if prev == true, as it use shared prevNode slice.
func (p *DB) findGE_NB(key int64, prev bool) (*mNode, bool) {
	x := p.head
	h := int(p.maxHeight) - 1
	for {
		next := x.getNext_NB(h)
		var cmp int64
		cmp = 1
		if next != nil {
			cmp = key - next.timeKey
		}
		if cmp < 0 {
			// Keep searching in this list
			x = next
		} else {
			if prev {
				p.prev[h] = x
			} else if cmp == 0 {
				return next, true
			}
			if h == 0 {
				return next, cmp == 0
			}
			h--
		}
	}
	return nil, false
}

func (p *DB) findLT(timeKey int64) *mNode {
	x := p.head
	h := int(atomic.LoadInt32(&p.maxHeight)) - 1
	for {
		next := x.getNext(h)
		if next == nil || (timeKey - next.timeKey) >= 0 {
			if h == 0 {
				if x == p.head {
					return nil
				}
				return x
			}
			h--
		} else {
			x = next
		}
	}
	return nil
}

func (p *DB) findLast() *mNode {
	x := p.head
	h := int(atomic.LoadInt32(&p.maxHeight)) - 1
	for {
		next := x.getNext(h)
		if next == nil {
			if h == 0 {
				if x == p.head {
					return nil
				}
				return x
			}
			h--
		} else {
			x = next
		}
	}
	return nil
}

func (p *DB) randHeight() (h int32) {
	const branching = 4
	h = 1
	for h < tMaxHeight && p.rnd.Int() % branching == 0 {
		h++
	}
	return
}
