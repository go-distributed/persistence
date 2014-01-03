package store

import (
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/go-epaxos/message/example"
)

func TestWriteAndRead(t *testing.T) {
	f := initFile(t)
	s := NewStore(f)

	src := NewPreAcceptSample()
	if err := s.Write(src); err != nil {
		closeAndDeleteFile(f, t)
		t.Fatal(err)
	}
	// normally, we won't explicitly close a storage since we flush it after each write
	// in this test, we just emulate the system crash by closing the storage
	f.Close()

	// Try to recover
	f = openFile(t)
	defer closeAndDeleteFile(f, t)

	s = NewStore(f)
	dst := new(example.PreAccept)
	if err := s.Read(dst); err != nil {
		t.Fatal(err)
	}
	compareObj(src, dst, t)
}

func TestWriteAndReadAll(t *testing.T) {
	f := initFile(t)
	s := NewStore(f)

	src := make([]*example.PreAccept, rand.Intn(100))
	for i := range src {
		src[i] = NewPreAcceptSample()
	}

	for i := range src {
		if err := s.Write(src[i]); err != nil {
			closeAndDeleteFile(f, t)
			t.Fatal(err)
		}
	}
	f.Close()

	// Try to recover
	f = openFile(t)
	defer closeAndDeleteFile(f, t)

	s = NewStore(f)
	var dst []*example.PreAccept
	if err := s.ReadAll(&dst); err != nil {
		t.Fatal(err)
	}
	compareObj(src, dst, t)
}

func openFile(t *testing.T) *os.File {
	file, err := os.OpenFile("/tmp/temp.txt", os.O_RDONLY, 0666)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func initFile(t *testing.T) *os.File {
	file, err := os.OpenFile("/tmp/temp.txt", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func closeAndDeleteFile(f *os.File, t *testing.T) {
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove("/tmp/temp.txt"); err != nil {
		t.Fatal(err)
	}
}

func compareObj(src, dst interface{}, t *testing.T) {
	if !reflect.DeepEqual(src, dst) {
		t.Fatal("src dst not equal!")
	}
}

func NewPreAcceptSample() *example.PreAccept {
	this := &example.PreAccept{}
	this.LeaderId = rand.Int31()
	this.Replica = rand.Int31()
	this.Instance = rand.Int31()
	this.Ballot = rand.Int31()
	v5 := rand.Intn(100)
	this.Command = make([]byte, v5)
	for i := 0; i < v5; i++ {
		this.Command[i] = byte(rand.Intn(256))
	}
	this.Seq = rand.Int31()
	v6 := 5
	this.Deps = make([]int32, v6)
	for i := 0; i < v6; i++ {
		this.Deps[i] = rand.Int31()
	}
	return this
}
