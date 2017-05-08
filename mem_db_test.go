package benzene

import (
	"fmt"
	"testing"
	"time"
	"strconv"
)

func TestMemDB_NewDB(t *testing.T) {
	db := NewDB()
	timeKey := time.Now().Unix() + 1
	db.Put(timeKey, "a", 0.1)
	//fmt.Println(db.Get(timeKey, "a"))
}

func TestDB_GetRange(t *testing.T) {
	db := NewDB()
	timeKey := int64(100000)
	for i := 0; i < 30; i++ {
		timeKey += 1
		db.Put(timeKey, "test", float64(i))
	}
	fmt.Println("Insert success")
	timeKey_2 := int64(100009)
	resKey, resValue,  _ := db.GetRange(timeKey_2, timeKey_2+30, "test")
	for i:= 0; i<len(resKey); i++ {
		fmt.Println("Key", resKey[i], "value", resValue[i])
	}
}

func BenchmarkMemDB_Put(b *testing.B) {
	db := NewDB()
	startTime := time.Now().Unix()
	timeKey := time.Now().Unix()
	for i := 0; i < b.N; i++ {
		timeKey += int64(i)
		db.Put(timeKey, strconv.Itoa(i), 0.1)
	}
	endTime := time.Now().Unix()
	fmt.Println(endTime - startTime)
}
