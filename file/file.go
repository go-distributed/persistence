package file

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"unsafe"

	"github.com/go-epaxos/persistence"
)

// [*] all the size of "length" are 32bits, this time we do not support objects larger then 4 GB.
// We think this is reasonable if snapshot is in effect
type Size uint32

// A wrapper for os.File, implements the "persistence.Persistent" interface
type File struct {
	fp *os.File
	rw *bufio.ReadWriter
}

func NewFile(f *os.File) persistence.Persistent {
	return &File{
		fp: f,
		rw: bufio.NewReadWriter(
			bufio.NewReader(f),
			bufio.NewWriter(f)),
	}
}

// NewPersistentFile() returns a pointer to the "persistence.Persistentce" struct
// the type of the underlying io of the struct is *persistence.file.File
func NewPersistentFile(f *os.File) *persistence.Persistence {
	return persistence.NewPersistence(NewFile(f))
}

// Write() Writes one serializable object to the file
func (f *File) Write(obj persistence.Serializable) error {
	bytes, err := obj.Marshal()
	if err != nil {
		return err
	}
	err = binary.Write(f.rw, binary.LittleEndian, Size(len(bytes))) // head
	if err != nil {
		return err
	}
	_, err = f.rw.Write(bytes) // body
	if err != nil {
		return err
	}
	err = binary.Write(f.rw, binary.LittleEndian, Size(len(bytes))) //tail
	if err != nil {
		return err
	}
	return f.rw.Flush()
}

// Read() reads one serializable object from the file
func (f *File) Read(obj persistence.Serializable) error {
	var size Size

	err := binary.Read(f.rw, binary.LittleEndian, &size) // read head
	if err != nil {
		return err
	}
	bytes := make([]byte, size)
	_, err = io.ReadFull(f.rw, bytes) // read body
	if err != nil {                   // TODO: can we fix the error?
		return err
	}
	err = binary.Read(f.rw, binary.LittleEndian, &size) // read tail
	if err != nil {
		return err
	}
	return obj.Unmarshal(bytes)
}

// seekOne() seeks one item in either direction from current position.
// This funcitons helps to implement the Seek()
func (f *File) seekOne(direct int) error {
	var size Size

	if direct != 1 && direct != -1 {
		return errors.New("persistence.file.SeekOne: invalid direct, only 1 or -1 are permitted")
	}

	if direct == -1 { // move cursor to the head of the last element
		s := int64(unsafe.Sizeof(size))
		_, err := f.fp.Seek(-s, 1)
		if err != nil {
			return err
		}
	}
	// [*] Here if we use f.rw, the bufio will read more bytes than we want, thus we use f.fp
	err := binary.Read(f.fp, binary.LittleEndian, &size)
	if err != nil {
		return err
	}

	var offset int64
	switch direct {
	case 1: // seek forward
		offset = int64(size + Size(unsafe.Sizeof(size)))
		break
	case -1: // seek backward
		offset = -1 * int64(size+Size(2*unsafe.Sizeof(size)))
		break
	default:
		return errors.New("persistence.file.SeekOne: should not get here")
	}
	_, err = f.fp.Seek(offset, 1)
	return err
}

// Seek() sets the offset for the next Read and Write on the file to offset,
// the unit of the offset is "item", not byte, it sets the offset
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
// [*] This function is not that efficient this time due to we cannot know all objects' size beforehand.
// The time complexity is O(offset), and it will do O(offset) disc read.
func (f *File) Seek(offset int64, whence int) error {
	switch whence {
	case 0:
		_, err := f.fp.Seek(0, 0) // move cursor to head
		if err != nil {
			return err
		}
		break
	case 1:
		break
	case 2:
		_, err := f.fp.Seek(0, 2) // move cursor to tail
		if err != nil {
			return err
		}
		break
	default:
		return errors.New("persistence.file.Seek: invalid argument for whence")
	}

	var direct int = 1
	if offset < 0 {
		direct = -1
		offset = -offset // make offset positive
	}
	for i := int64(0); i < offset; i++ {
		err := f.seekOne(direct)
		if err != nil {
			return err
		}
	}
	// refresh the buffer to update the cursor
	f.rw = bufio.NewReadWriter(
		bufio.NewReader(f.fp),
		bufio.NewWriter(f.fp))

	return nil
}

// Truncate() truncates the file from the current cursor
func (f *File) Truncate() error {
	offset, err := f.fp.Seek(0, 1)
	if err != nil {
		return err
	}
	if err := f.fp.Truncate(offset); err != nil {
		return err
	}
	// refresh the buffer
	f.rw = bufio.NewReadWriter(
		bufio.NewReader(f.fp),
		bufio.NewWriter(f.fp))

	return nil
}
