package file

import (
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/go-epaxos/message/example"
)

// Write one object to file, close the file,
// then open the file and read the object
// and compare it to the origin
func TestWriteAndRead(t *testing.T) {
	f := initFile(t)
	pf := NewPersistentFile(f)

	src := NewPreAcceptSample()
	if err := pf.Write(src); err != nil {
		closeAndDeleteFile(f, t)
		t.Fatal(err)
	}
	// normally, we won't explicitly close a storage since we flush it after each write
	// in this test, we just emulate the system crash by closing the storage
	f.Close()

	// Try to recover
	f = openFile(t)
	defer closeAndDeleteFile(f, t)

	pf = NewPersistentFile(f)
	dst := new(example.PreAccept)
	if err := pf.Read(dst); err != nil {
		t.Fatal(err)
	}
	compareObj(src, dst, t)
}

// Write a group objects to file, close file,
// then open the file and read out all the objects
// and compare those objects to the origins
func TestWriteAndReadAll(t *testing.T) {
	f := initFile(t)
	pf := NewPersistentFile(f)

	src := make([]*example.PreAccept, rand.Intn(100)+10)
	for i := range src {
		src[i] = NewPreAcceptSample()
	}

	for i := range src {
		if err := pf.Write(src[i]); err != nil {
			closeAndDeleteFile(f, t)
			t.Fatal(err)
		}
	}
	f.Close()

	// Try to recover
	f = openFile(t)
	defer closeAndDeleteFile(f, t)

	pf = NewPersistentFile(f)
	var dst []*example.PreAccept
	if err := pf.ReadAll(&dst); err != nil {
		t.Fatal(err)
	}
	compareObj(src, dst, t)
}

// Write a group of objects to the file,
// then seek the objects and compare them to the origin.
func TestSeekAndRead(t *testing.T) {
	// prepare
	f := initFile(t)
	pf := NewPersistentFile(f)
	defer closeAndDeleteFile(f, t)

	src := make([]*example.PreAccept, rand.Intn(100)+10)
	for i := range src {
		src[i] = NewPreAcceptSample()
	}

	for i := range src {
		if err := pf.Write(src[i]); err != nil {
			t.Fatal(err)
		}
	}

	// test seeking backward
	for i := range src {
		err := pf.Seek(0, 2)
		if err != nil {
			t.Fatal(err)
		}
		err = pf.Seek(int64(-i-1), 1)
		if err != nil {
			t.Fatal(err)
		}
		dst := new(example.PreAccept)
		if err = pf.Read(dst); err != nil {
			t.Fatal(err)
		}
		compareObj(src[len(src)-i-1], dst, t)
	}

	// test seeking forward
	for i := range src {
		err := pf.Seek(0, 0)
		if err != nil {
			t.Fatal(err)
		}
		err = pf.Seek(int64(i), 1)
		if err != nil {
			t.Fatal(err)
		}
		dst := new(example.PreAccept)
		if err = pf.Read(dst); err != nil {
			t.Fatal(err)
		}
		compareObj(src[i], dst, t)
	}
}

// Write a group of objects to the file,
// then seek and rewrite the objects
// and read out them one by one and do comparision
func TestSeekAndWrite(t *testing.T) {
	// prepare
	f := initFile(t)
	pf := NewPersistentFile(f)
	defer closeAndDeleteFile(f, t)

	arrayLen := rand.Intn(100) + 10
	src := make([]*example.PreAccept, arrayLen)
	secondSrc := make([]*example.PreAccept, arrayLen)
	for i := range src {
		src[i] = NewPreAcceptSample()
		secondSrc[i] = NewPreAcceptSample()
	}
	for i := range src {
		if err := pf.Write(src[i]); err != nil {
			t.Fatal(err)
		}
	}

	// test seeking forward
	for i, _ := range src {
		err := pf.Seek(0, 0)
		if err != nil {
			t.Fatal(err)
		}
		err = pf.Seek(int64(i), 1)
		if err != nil {
			t.Fatal(err)
		}
		if err = pf.Write(secondSrc[i]); err != nil {
			t.Fatal(err)
		}
	}

	// compare
	err := pf.Seek(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := range secondSrc {
		dst := new(example.PreAccept)
		if err := pf.Read(dst); err != nil {
			t.Fatal(err)
		}
		compareObj(secondSrc[i], dst, t)
	}
}

// Write a group of objects to the file,
// then truncate at a random offset, and write new stuffs,
// finally read out those objects and do comparision
func TestTruncate(t *testing.T) {
	// prepare
	f := initFile(t)
	pf := NewPersistentFile(f)
	defer closeAndDeleteFile(f, t)

	arrayLen := rand.Intn(100) + 10
	src := make([]*example.PreAccept, arrayLen)
	secondSrc := make([]*example.PreAccept, arrayLen)
	for i := range src {
		src[i] = NewPreAcceptSample()
		secondSrc[i] = NewPreAcceptSample()
	}
	for i := range src {
		if err := pf.Write(src[i]); err != nil {
			t.Fatal(err)
		}
	}

	// test truncate
	truncMagic := rand.Intn(arrayLen) // from where to truncate
	err := pf.Seek(int64(truncMagic), 0)
	if err != nil {
		t.Fatal(err)
	}
	err = pf.Truncate()
	if err != nil {
		t.Fatal(err)
	}
	for i := truncMagic; i < len(secondSrc); i++ { // write new stuff from the truncate point
		if err := pf.Write(secondSrc[i]); err != nil {
			t.Fatal(err)
		}
	}
	err = pf.Seek(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var dst []*example.PreAccept
	if err := pf.ReadAll(&dst); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < truncMagic; i++ {
		compareObj(src[i], dst[i], t)
	}
	for i := truncMagic; i < len(secondSrc); i++ {
		compareObj(secondSrc[i], dst[i], t)
	}
}

// Below are some helper functions for init and clean-up
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
		t.Fatal("Not equal!")
	}
}

func NewPreAcceptSample() *example.PreAccept {
	this := &example.PreAccept{}
	this.LeaderId = rand.Int31()
	this.Replica = rand.Int31()
	this.Instance = rand.Int31()
	this.Ballot = rand.Int31()
	v5 := rand.Intn(100) + 1
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
