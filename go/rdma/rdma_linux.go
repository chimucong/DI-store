//+build linux
package rdma

/*
#cgo linux LDFLAGS: -L${SRCDIR} -lpetrel_ib -libverbs -lstdc++
#include "ib_c.h"
size_t ConnectionInfoSize = sizeof(IBConnectionInfo);
size_t IBBufSize = sizeof(IBBuf);
*/
import "C"
import (
	"context"
	"runtime"
	"unsafe"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type Device struct {
	device C.IBDevicePtr
}

type Context struct {
	context C.IBContextPtr
}

type ConnectionInfo struct {
	info C.IBConnectionInfo
}

func (info *ConnectionInfo) ToBytes() []byte {
	b := make([]byte, C.ConnectionInfoSize, C.ConnectionInfoSize)
	C.memcpy(unsafe.Pointer(&b[0]), unsafe.Pointer(&info.info), C.ConnectionInfoSize)
	return b
}

func ConnectionInfoFromBytes(b []byte) *ConnectionInfo {
	if len(b) != int(C.ConnectionInfoSize) {
		panic("ConnectionInfoFromBytes: size dose not match")
	}
	info := &ConnectionInfo{}
	C.memcpy(unsafe.Pointer(&info.info), unsafe.Pointer(&b[0]), C.ConnectionInfoSize)
	return info
}

type Buf struct {
	buf C.IBBuf
}

func (buf *Buf) ToBytes() []byte {
	b := make([]byte, C.IBBufSize, C.IBBufSize)
	C.memcpy(unsafe.Pointer(&b[0]), unsafe.Pointer(&buf.buf), C.IBBufSize)
	return b
}

func BufFromBytes(b []byte) *Buf {
	if len(b) != int(C.IBBufSize) {
		panic("BufFromBytes: size dose not match")
	}
	buf := &Buf{}
	C.memcpy(unsafe.Pointer(&buf.buf), unsafe.Pointer(&b[0]), C.IBBufSize)
	return buf
}

func msgToError(cmsg *C.char) error {
	msg := C.GoString(cmsg)
	if msg == "" {
		return nil
	} else {
		return errors.Errorf(msg)
	}
}

func OpenDevice(name string, ibPort uint8, gidIndex uint8) (*Device, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	device := &Device{}
	msg := C.IBOpenDevice(cname, C.uchar(ibPort), C.uchar(gidIndex), &device.device)
	err := msgToError(msg)
	if err != nil {
		return nil, err
	}
	runtime.SetFinalizer(device, deviceFinalizer)
	return device, nil
}

func deviceFinalizer(device *Device) {
	if device.device != nil {
		device.Close()
	}
}

func (device *Device) Close() error {
	if device.device == nil {
		return nil
	}
	msg := C.IBCloseDevice(device.device)
	device.device = nil
	return msgToError(msg)
}

func (device *Device) NewContext(ctx context.Context) (*Context, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Device.NewContext")
	defer span.Finish()
	context := &Context{}
	msg := C.IBNewContext(device.device, &context.context)
	err := msgToError(msg)
	if err != nil {
		return nil, err
	}
	return context, nil
}

func (context *Context) Close(ctx context.Context) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Context.Close")
	defer span.Finish()
	msg := C.IBCloseContext(context.context)
	return msgToError(msg)
}

func (context *Context) RegMr(ctx context.Context, buf []byte) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Context.RegMr")
	defer span.Finish()
	msg := C.IBRegMr(context.context, unsafe.Pointer(&buf[0]), C.long(len(buf)))
	return msgToError(msg)
}

func (context *Context) UnRegMr(ctx context.Context) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Context.UnRegMr")
	defer span.Finish()
	msg := C.IBUnRegMr(context.context)
	return msgToError(msg)
}

func (context *Context) Connect(ctx context.Context, info *ConnectionInfo) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Context.Connect")
	defer span.Finish()
	msg := C.IBConnect(context.context, &info.info)
	return msgToError(msg)
}

func (context *Context) postRead_(ctx context.Context,
	laddr unsafe.Pointer, raddr unsafe.Pointer, length uint32,
	rkey C.uint32_t, send_signaled bool) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Context.postRead_")
	defer span.Finish()

	log.Debugf("laddr: %d, raddr: %d, length: %d, rkey: %d", laddr, raddr, length, rkey)
	msg := C.IBPostRead(context.context,
		laddr, raddr, C.uint32_t(length), rkey, C.bool(send_signaled))
	return msgToError(msg)
}

func (context *Context) Read(ctx context.Context, buf *Buf) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Context.Read")
	defer span.Finish()

	lBuf, err := context.GetBuf()
	if err != nil {
		return nil
	}
	if lBuf.buf.length != buf.buf.length {
		return errors.New("RDMA memory region does not match")
	}
	sizeLeft := uint64(lBuf.buf.length)
	laddr := lBuf.buf.addr
	raddr := buf.buf.addr
	rkey := buf.buf.rkey
	for sizeLeft > 0 {
		var l uint64 = MR_SEGMENT_MAX
		if sizeLeft < l {
			l = sizeLeft
		}
		err = context.postRead_(ctx, laddr, raddr, uint32(l), rkey, true)
		if err != nil {
			return nil
		}
		err = context.Poll(ctx, POLL_TIMEOUT_MS)
		if err != nil {
			return nil
		}
		laddr = unsafe.Pointer(uintptr(laddr) + uintptr(l))
		raddr = unsafe.Pointer(uintptr(raddr) + uintptr(l))
		sizeLeft -= l
	}

	return nil
}

func (context *Context) PostSendEmpty(ctx context.Context, send_signaled bool) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Context.PostSendEmpty")
	defer span.Finish()

	msg := C.IBPostSendEmpty(context.context, C.bool(send_signaled))
	return msgToError(msg)
}

func (context *Context) PostRecvEmpty(ctx context.Context) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Context.PostRecvEmpty")
	defer span.Finish()

	msg := C.IBPostRecvEmpty(context.context)
	return msgToError(msg)
}

func (context *Context) Poll(ctx context.Context, timeout_ms int) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Context.Poll")
	defer span.Finish()

	msg := C.IBPoll(context.context, C.int(timeout_ms))
	return msgToError(msg)
}

func (context *Context) GetConnectionInfo() (*ConnectionInfo, error) {
	info := &ConnectionInfo{}
	msg := C.IBGetConnectionInfo(context.context, &info.info)
	err := msgToError(msg)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (context *Context) GetBuf() (*Buf, error) {
	buf := &Buf{}
	msg := C.IBGetBuf(context.context, &buf.buf)
	err := msgToError(msg)
	if err != nil {
		return nil, err
	}
	return buf, nil
}
