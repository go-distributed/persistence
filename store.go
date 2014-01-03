package store

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
)

// To store the command in durable storage
type Store struct {
	rw *bufio.ReadWriter
}

type Serializable interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

func NewStore(rw io.ReadWriter) *Store {
	return &Store{bufio.NewReadWriter(bufio.NewReader(rw), bufio.NewWriter(rw))}
}

// Write one Serializable object to the Writer
func (s *Store) Write(obj Serializable) error {
	bytes, err := obj.Marshal()
	if err != nil {
		return err
	}
	err = binary.Write(s.rw, binary.LittleEndian, uint32(len(bytes)))
	if err != nil {
		return err
	}
	_, err = s.rw.Write(bytes)
	return s.rw.Flush()
}

// Read one Serializable object from the Reader
func (s *Store) Read(obj Serializable) error {
	var size uint32
	err := binary.Read(s.rw, binary.LittleEndian, &size)
	if err != nil {
		return err
	}
	bytes := make([]byte, size)
	_, err = io.ReadFull(s.rw, bytes)
	if err != nil { // TODO: can we fix the error?
		return err
	}
	return obj.Unmarshal(bytes)
}

// Read all Serializable objects from the Reader
// objs must be a ptr to a slice, the slice's element should be serializable, i.e.
// implements Marshal() and Unmarshal()
func (s *Store) ReadAll(addr interface{}) error {
	t := reflect.TypeOf(addr) // t should be a ptr type
	if t.Kind() != reflect.Ptr {
		return errors.New("store.ReadAll: invalid argument, not a ptr type")
	}

	t = t.Elem() // now t should be a slice type
	if t.Kind() != reflect.Slice {
		return errors.New("store.ReadAll: invalid argument, not a ptr to slice")
	}
	slice := reflect.MakeSlice(t, 0, 0)

	t = t.Elem()                 // t is now the element type, it can be a ptr or non-ptr
	if t.Kind() == reflect.Ptr { // get the underlying type
		t = t.Elem()
	}
	v := reflect.New(t) // v is guranteed to be a ptr to the element type
	obj, ok := v.Interface().(Serializable)
	if !ok {
		return errors.New("store.ReadAll: invalid argument, not a serializable type")
	}

	// try to read out all the objects
	for {
		if err := s.Read(obj); err != nil {
			if err == io.EOF { // read finish
				break
			}
			return err
		}
		slice = reflect.Append(slice, reflect.ValueOf(obj))
		obj = reflect.New(t).Interface().(Serializable)
	}
	reflect.ValueOf(addr).Elem().Set(slice) // to make side-effect
	return nil
}
