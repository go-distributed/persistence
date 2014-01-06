package persistence

import (
	"errors"
	"io"
	"reflect"
)

type Serializable interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

type Persistent interface {
	Read(obj Serializable) error
	Write(obj Serializable) error
	Seek(offset int64, whence int) error
	Truncate() error
}

type Persistence struct {
	io Persistent
}

// Constructor
func NewPersistence(io Persistent) *Persistence {
	return &Persistence{io}
}

// Read() reads one serializable object from the io
func (p *Persistence) Read(obj Serializable) error {
	return p.io.Read(obj)
}

// Write() writes one serializable object to the io
func (p *Persistence) Write(obj Serializable) error {
	return p.io.Write(obj)
}

// ReadAll() reads all serializable objects from the underlying io
// addr must be a ptr to a slice, and the slice's element should be serializable, i.e.
// implements Marshal() and Unmarshal()
func (p *Persistence) ReadAll(addr interface{}) error {
	t := reflect.TypeOf(addr)
	if t.Kind() != reflect.Ptr { // test if addr is a ptr type
		return errors.New("persistence.ReadAll: invalid argument, not a ptr type")
	}

	t = t.Elem()
	if t.Kind() != reflect.Slice {
		return errors.New("persistence.ReadAll: invalid argument, not a ptr to slice")
	}

	t = t.Elem()

	// t is now the type of the array's elements,
	// it can be a ptr type or non-ptr type
	// if t is a ptr type, then try to get the underlying type
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	v := reflect.New(t)
	obj, ok := v.Interface().(Serializable)
	if !ok {
		return errors.New("persistence.ReadAll: invalid argument, not a serializable type")
	}

	// try to read out all the objects into slice
	slice := reflect.MakeSlice(t.Elem(), 0, 0)
	for {
		if err := p.io.Read(obj); err != nil {
			if err == io.EOF { // finish reading
				break
			}
			return err
		}
		slice = reflect.Append(slice, reflect.ValueOf(obj))
		obj = reflect.New(t).Interface().(Serializable)
	}
	reflect.ValueOf(addr).Elem().Set(slice) // save the result
	return nil
}

// Seek() sets the offset for the next operation on the underlying io to offset,
// the unit of the offset is "item", not byte, it sets the offset
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
func (p *Persistence) Seek(offset int64, whence int) error {
	return p.io.Seek(offset, whence)
}

// Truncate() truncates the io from current cursor
func (p *Persistence) Truncate() error {
	return p.io.Truncate()
}
