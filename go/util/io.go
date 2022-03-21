package util

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"unsafe"
)

type BytesReadWriter struct {
	conn net.Conn
}

func NewBytesReadWriter(conn net.Conn) *BytesReadWriter {
	return &BytesReadWriter{
		conn: conn,
	}
}

func (rw *BytesReadWriter) Write(p []byte) (int, error) {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(len(p)))
	n1, err := rw.conn.Write(bs)
	if err != nil {
		return n1, err
	}
	n2, err := rw.conn.Write(p)
	return n1 + n2, err
}

func (rw *BytesReadWriter) Read(buff []byte) ([]byte, error) {
	bs := make([]byte, 4)
	_, err := io.ReadFull(rw.conn, bs)
	if err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint32(bs)

	var data []byte
	if buff == nil {
		data = make([]byte, size)
	} else {
		if int(size) != len(buff) {
			panic("buff size mismatched")
		}
		data = buff
	}

	if size == 0 {
		return data, nil
	}
	_, err = io.ReadFull(rw.conn, data)
	return data, err
}

func (rw *BytesReadWriter) Close() error {
	return rw.conn.Close()
}

func BytesWithoutCopy(p unsafe.Pointer, size int) []byte {
	return (*[1 << 32]byte)(p)[:size:size]
}

const PAGE_SIZE = 1024 * 4

func WarmupBytes(buf []byte, block_size int, threads int) {
	size_total := len(buf)
	if threads <= 1 || size_total <= block_size {
		for i := 0; i < size_total; i += PAGE_SIZE {
			buf[i] = 0
		}
		return
	}

	size_per_thread := (size_total + threads - 1) / threads

	if size_per_thread < block_size {
		size_per_thread = block_size
	}
	size_per_thread = ((size_per_thread + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE

	var wg sync.WaitGroup
	offset := 0
	for offset < size_total {
		actual_size := size_total - offset
		if actual_size > size_per_thread {
			actual_size = size_per_thread
		}
		wg.Add(1)
		go func(start, length int) {
			for i := 0; i < length; i += PAGE_SIZE {
				buf[start+i] = 0
			}
			wg.Done()
		}(offset, actual_size)

		offset += actual_size

	}
	wg.Wait()
}
