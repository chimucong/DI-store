//go:build darwin
// +build darwin

package rdma

import (
	"context"

	"github.com/pkg/errors"
)

var NOT_IMPLEMENTED_ERROR = errors.New("not implemented: RDMA is not supported on darwin OS")

type Device struct {
}

type Context struct {
}

type ConnectionInfo struct {
}

func (info *ConnectionInfo) ToBytes() []byte {
	return nil
}

func ConnectionInfoFromBytes(b []byte) *ConnectionInfo {
	return nil
}

type Buf struct{}

func (buf *Buf) ToBytes() []byte {
	return nil
}

func BufFromBytes(b []byte) *Buf {
	return nil
}

func OpenDevice(name string, ibPort uint8, gidIndex uint8) (*Device, error) {
	return nil, NOT_IMPLEMENTED_ERROR
}

func (device *Device) Close() error {
	return NOT_IMPLEMENTED_ERROR
}

func (device *Device) NewContext(ctx context.Context) (*Context, error) {
	return nil, NOT_IMPLEMENTED_ERROR
}

func (context *Context) Close(ctx context.Context) error {
	return NOT_IMPLEMENTED_ERROR
}

func (context *Context) RegMr(ctx context.Context, buf []byte) error {
	return NOT_IMPLEMENTED_ERROR
}

func (context *Context) UnRegMr(ctx context.Context) error {
	return NOT_IMPLEMENTED_ERROR
}

func (context *Context) Connect(ctx context.Context, info *ConnectionInfo) error {
	return NOT_IMPLEMENTED_ERROR
}

func (context *Context) Read(ctx context.Context, buf *Buf) error {
	return NOT_IMPLEMENTED_ERROR
}

func (context *Context) PostSendEmpty(ctx context.Context, send_signaled bool) error {
	return NOT_IMPLEMENTED_ERROR
}

func (context *Context) PostRecvEmpty(ctx context.Context) error {
	return NOT_IMPLEMENTED_ERROR
}

func (context *Context) Poll(ctx context.Context, timeout_ms int) error {
	return NOT_IMPLEMENTED_ERROR
}

func (context *Context) GetConnectionInfo() (*ConnectionInfo, error) {
	return nil, NOT_IMPLEMENTED_ERROR
}

func (context *Context) GetBuf() (*Buf, error) {
	return nil, NOT_IMPLEMENTED_ERROR
}
