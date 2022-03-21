package storage_server

import (
	"bytes"
	"context"
	fb_object "di_store/fb/object"
	"di_store/node_tracker"
	pb_node_tracker "di_store/pb/node_tracker"
	pb_object_fetch "di_store/pb/object_fetch"
	pbObjectStore "di_store/pb/storage_server"
	"di_store/plasma_client"
	"di_store/rdma"
	"di_store/tracing"
	"di_store/util"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"

	"github.com/opentracing/opentracing-go"
	ot_log "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type TaskState int
type TaskFuture <-chan error

const (
	TASK_EXIST = iota
)

type FetchTaskManager struct {
	taskInProcess map[string][]chan error
	mu            sync.Mutex
}

type FetchCall func(ctx context.Context, oid []byte, srcNode string, srcNodeOnly bool) error
type FetchFn func(ctx context.Context, oid []byte, remoteServer *pb_node_tracker.StorageServer) error

func NewFetchTaskManager() *FetchTaskManager {
	return &FetchTaskManager{taskInProcess: make(map[string][]chan error)}
}

func (m *FetchTaskManager) CreateTask(ctx context.Context, oid string) TaskFuture {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FetchTaskManager.CreateTask")
	defer span.Finish()

	m.mu.Lock()
	defer m.mu.Unlock()

	l, exist := m.taskInProcess[oid]
	if exist {
		fut := make(chan error, 1)
		m.taskInProcess[oid] = append(l, fut)
		return fut
	} else {
		m.taskInProcess[oid] = nil
		return nil
	}
}

func (m *FetchTaskManager) Notify(ctx context.Context, oid string, err error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FetchTaskManager.Notify")
	defer span.Finish()

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, q := range m.taskInProcess[oid] {
		q <- err
	}
	delete(m.taskInProcess, oid)
}

func (m *FetchTaskManager) Wait(ctx context.Context, f TaskFuture) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FetchTaskManager.Wait")
	defer span.Finish()
	return <-f
}

func (m *FetchTaskManager) Fetch(ctx context.Context, oid []byte, srcNode string, srcNodeOnly bool, f FetchCall) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FetchTaskManager.Fetch")
	defer span.Finish()

	key := string(oid)
	fut := m.CreateTask(ctx, key)
	if fut != nil {
		return m.Wait(ctx, fut)
	}
	err := f(ctx, oid, srcNode, srcNodeOnly)
	m.Notify(ctx, key, err)
	return err
}

func (server *StorageServer) getFetchServerList(ctx context.Context, oid []byte) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "StorageServer.fetchServerList")
	defer span.Finish()
	client, err := node_tracker.NewNodeTrackerClient(ctx, server.NodeTrackerRpcTarget)
	if err != nil {
		return nil, err
	}
	objInfoList, err := client.ObjectInfo(ctx, hex.EncodeToString(oid))
	if err != nil {
		return nil, err
	}

	objInfo := objInfoList[0]
	serverList := objInfo.ServerHostnameList
	rand.Shuffle(len(serverList), func(i, j int) {
		serverList[i], serverList[j] = serverList[j], serverList[i]
	})
	return serverList, nil
}

func (server *StorageServer) fetchWithRetry(ctx context.Context, oid []byte, srcNode string, srcNodeOnly bool, fetch FetchFn) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "StorageServer.fetchWithRetry")
	defer span.Finish()

	var success = false
	var lastErr error = nil

	if srcNode != "" {
		log.Debugf("try fetch %v from node %v", hex.EncodeToString(oid), srcNode)
		err := func() error {
			serverInfo, err := server.GetServerInfo(ctx, srcNode)
			if err != nil {
				return err
			}
			return fetch(ctx, oid, serverInfo)
		}()
		if err == nil {
			success = true
		} else {
			log.Warningf("%v", err)
			lastErr = err
		}
	}

	if !success && !srcNodeOnly {
		// todo remove srcNode from remoteServerList
		remoteServerList, err := server.getFetchServerList(ctx, oid)
		if err != nil {
			return err
		}

		for _, remoteServer := range remoteServerList {
			serverInfo, err := server.GetServerInfo(ctx, remoteServer)
			if err != nil {
				lastErr = err
				continue
			}
			err = fetch(ctx, oid, serverInfo)
			if err != nil {
				lastErr = err
			} else {
				success = true
				break
			}
		}
	}

	if !success {
		return lastErr
	}

	client, err := node_tracker.NewNodeTrackerClient(ctx, server.NodeTrackerRpcTarget)
	if err != nil {
		return err
	}
	_, err = client.RegisterObject(ctx, hex.EncodeToString(oid), server.Hostname)
	return err
}

func (server *StorageServer) fetchViaSocket(ctx context.Context, oid []byte, srcNode string, srcNodeOnly bool) error {
	return server.fetchWithRetry(ctx, oid, srcNode, srcNodeOnly, server.tryFetchViaSocket)
}

func (server *StorageServer) fetchViaRdma(ctx context.Context, oid []byte, srcNode string, srcNodeOnly bool) error {
	return server.fetchWithRetry(ctx, oid, srcNode, srcNodeOnly, server.tryFetchViaRdma)
}

func (server *StorageServer) tryFetchViaRdma(ctx context.Context, oid []byte, remoteServerInfo *pb_node_tracker.StorageServer) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "StorageServer.tryFetchViaRdma")
	defer span.Finish()

	target := remoteServerInfo.GetObjTransferTarget()

	_span, _ := opentracing.StartSpanFromContext(ctx, "net.Dial")
	conn, err := net.Dial("tcp", target)
	if err != nil {
		_span.LogFields(ot_log.Error(err))
		_span.Finish()
		return errors.Wrapf(err, "net.Dial %v", target)
	}
	_span.Finish()
	defer conn.Close()

	rw := util.NewBytesReadWriter(conn)
	var spanCtx []byte
	if tracing.Enabled {
		var buf bytes.Buffer
		writer := io.Writer(&buf)
		err := span.Tracer().Inject(span.Context(), opentracing.Binary, writer)
		if err != nil {
			err = errors.Wrap(err, "tryFetchViaRdma: inject span error")
			log.Error("%+v", err)
		} else {
			spanCtx = buf.Bytes()
		}
	}
	rdmaSupport := server.RdmaDevice != nil
	header := &pb_object_fetch.FetchHeader{
		SpanCtx:     spanCtx,
		ObjectId:    oid,
		RdmaSupport: rdmaSupport,
	}
	buf, err := proto.Marshal(header)
	if err != nil {
		return err
	}
	_span, _ = opentracing.StartSpanFromContext(ctx, "request.write.header")
	_, err = rw.Write(buf)
	_span.Finish()
	if err != nil {
		return err
	}

	_span, _ = opentracing.StartSpanFromContext(ctx, "response.read.header")
	buf, err = rw.Read(nil)
	_span.Finish()
	if err != nil {
		return err
	}
	err = proto.Unmarshal(buf, header)
	if err != nil {
		return err
	}

	if err := header.GetErrorMsg(); len(err) > 0 {
		err := errors.Errorf("fetch error from remote: %v, %v", target, string(err))
		log.Error(err)
		return err
	}

	if header.GetObjectLength() < 0 {
		err := errors.Wrapf(util.ErrNotFound, "object %s not found on server %s", hex.EncodeToString(oid), target)
		return err
	}

	chCreateBuf := make(chan error, 1)
	chRdmaCtx := make(chan *rdma.Context, 1)

	var plasmaBuff *plasma_client.Buff
	defer func() {
		plasmaBuff.Release(ctx)
	}()

	var objectHanging bool
	defer func() {
		if objectHanging {
			server.PlasmaClient.Abort(ctx, oid)
		}
	}()
	go func() {
		var err1 error
		plasmaBuff, err1 = server.PlasmaClient.Create(ctx, oid, int(header.GetObjectLength()))
		if err1 != nil {
			chCreateBuf <- err1
			return
		}
		objectHanging = true
		if header.GetRdmaSupport() {
			rdmaCtx := <-chRdmaCtx
			if rdmaCtx != nil {
				buffSlice := plasmaBuff.ToByteSlice()
				if server.RdmaMemWarmup {
					span, _ = opentracing.StartSpanFromContext(ctx, "util.WarmupBytes")
					util.WarmupBytes(buffSlice, server.RdmaMemWarmupChunk, server.RdmaMemWarmupThread)
					span.LogFields(
						ot_log.Int("chunk_size", server.RdmaMemWarmupChunk),
						ot_log.Int("thread_number", server.RdmaMemWarmupThread),
					)
					span.Finish()
				}
				chCreateBuf <- rdmaCtx.RegMr(ctx, buffSlice)
				return
			}
		}
		chCreateBuf <- nil

	}()

	if header.GetRdmaSupport() {
		log.Debug("fetch via rdma")
		rdmaCtx, err := server.RdmaDevice.NewContext(ctx)
		chRdmaCtx <- rdmaCtx
		if err != nil {
			return err
		}
		defer rdmaCtx.Close(ctx)

		connInfo, err := rdmaCtx.GetConnectionInfo()
		if err != nil {
			return err
		}

		connInfoMsg := &pb_object_fetch.ConnectionInfo{Info: connInfo.ToBytes()}
		buf, err := proto.Marshal(connInfoMsg)
		if err != nil {
			return err
		}
		_, err = rw.Write(buf)
		if err != nil {
			return err
		}

		buf, err = rw.Read(nil)
		if err != nil {
			return err
		}

		err = proto.Unmarshal(buf, connInfoMsg)
		if err != nil {
			return err
		}
		connInfo = rdma.ConnectionInfoFromBytes(connInfoMsg.GetInfo())

		err = rdmaCtx.Connect(ctx, connInfo)
		if err != nil {
			return err
		}

		buf, err = rw.Read(nil)
		if err != nil {
			return err
		}
		rBufMsg := pb_object_fetch.RBuf{}
		err = proto.Unmarshal(buf, &rBufMsg)
		if err != nil {
			return err
		}
		rBuf := rdma.BufFromBytes(rBufMsg.GetRbuf())
		err = <-chCreateBuf
		if err != nil {
			return err
		}
		err = rdmaCtx.Read(ctx, rBuf)
		if err != nil {
			return err
		}
		rdmaCtx.PostSendEmpty(ctx, true)
		err = rdmaCtx.Poll(ctx, rdma.POLL_TIMEOUT_MS)
		if err != nil {
			return err
		}
	} else {
		err = <-chCreateBuf
		if err != nil {
			return err
		}
		_span, _ = opentracing.StartSpanFromContext(ctx, "response.read.socket_data")
		_, err = rw.Read(plasmaBuff.ToByteSlice())
		_span.Finish()
		if err != nil {
			return err
		}
	}

	span.LogFields(ot_log.Int("object_length", int(header.GetObjectLength())))
	objectHanging = false
	return server.PlasmaClient.Seal(ctx, oid)
}

func (server *StorageServer) tryFetchViaSocket(ctx context.Context, oid []byte, remoteServerInfo *pb_node_tracker.StorageServer) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "StorageServer.tryFetchViaSocket")
	defer span.Finish()

	target := remoteServerInfo.GetObjTransferTarget()

	_span, _ := opentracing.StartSpanFromContext(ctx, "net.Dial")
	conn, err := net.Dial("tcp", target)
	if err != nil {
		_span.LogFields(ot_log.Error(err))
		_span.Finish()
		return errors.Wrapf(err, "net.Dial %v", target)
	}
	_span.Finish()
	defer conn.Close()

	rw := util.NewBytesReadWriter(conn)
	var spanCtx []byte
	if tracing.Enabled {
		var buf bytes.Buffer
		writer := io.Writer(&buf)
		err := span.Tracer().Inject(span.Context(), opentracing.Binary, writer)
		if err != nil {
			err = errors.Wrap(err, "tryFetchViaSocket: inject span error")
			log.Error("%+v", err)
		} else {
			spanCtx = buf.Bytes()
		}
	}

	buf := fb_object.FetchTaskHeaderToBytes(oid, -1, -1, -1, spanCtx, nil)

	_span, _ = opentracing.StartSpanFromContext(ctx, "request.write.meta")
	_, err = rw.Write(buf)
	_span.Finish()
	if err != nil {
		return err
	}

	_span, _ = opentracing.StartSpanFromContext(ctx, "response.read.meta")
	buf, err = rw.Read(nil)
	_span.Finish()
	if err != nil {
		return err
	}

	header := fb_object.BytesToFetchTaskHeader(buf)
	if err := header.ErrorBytes(); len(err) > 0 {
		err := errors.Errorf("fetch error from remote: %v, %v", target, string(err))
		log.Error(err)
		return err
	}

	meta := header.ObjectMeta(nil)
	objectLength := int(meta.ObjectLength())
	if objectLength == -1 {
		err := errors.Wrapf(util.ErrNotFound, "object %s not found on server %s", hex.EncodeToString(oid), target)
		return err
	}

	buff, err := server.PlasmaClient.Create(ctx, oid, objectLength)
	defer buff.Release(ctx)

	if err != nil {
		return err
	}
	_span, _ = opentracing.StartSpanFromContext(ctx, "response.read.data")
	_, err = rw.Read(buff.ToByteSlice())
	_span.Finish()
	if err != nil {
		server.PlasmaClient.Abort(ctx, oid)
		return err
	}

	span.LogFields(ot_log.Int("object_length", objectLength))
	return server.PlasmaClient.Seal(ctx, oid)
}

func (server *StorageServer) objTransferListenLoop() {
	log.Info("storage server, listening object transfer request on port ", server.ObjTransferListener.Addr())
	for {
		conn, err := server.ObjTransferListener.Accept()
		if err != nil {
			log.Error(fmt.Errorf("objTransferListenLoop %v", err))
		}
		go server.processObjTransferRequest(context.Background(), conn)
	}
}

func (server *StorageServer) processObjTransferRequest(ctx context.Context, conn net.Conn) {
	log.Debugf("processObjTransferRequest start, remote: %v", conn.RemoteAddr().String())

	rw := util.NewBytesReadWriter(conn)
	defer conn.Close()

	var oid, data []byte
	var finErr error
	var span opentracing.Span
	var remoteRdmaSupport bool
	{
		buf, err := rw.Read(nil)
		if err != nil {
			finErr = errors.Wrapf(err, "socket read")
			goto FIN
		}
		header := &pb_object_fetch.FetchHeader{}
		err = proto.Unmarshal(buf, header)
		if err != nil {
			finErr = errors.Wrapf(err, "proto.Unmarshal")
			goto FIN
		}
		remoteRdmaSupport = header.GetRdmaSupport()
		if err := header.GetErrorMsg(); len(err) > 0 {
			finErr = errors.New(err)
			goto FIN
		}

		if tracing.Enabled {
			tracer := opentracing.GlobalTracer()
			if spanCtxBytes := header.GetSpanCtx(); len(spanCtxBytes) > 0 {
				carrier := bytes.NewReader(spanCtxBytes)
				if sCtx, err := tracer.Extract(opentracing.Binary, carrier); err == nil {
					span = tracer.StartSpan("processObjTransferRequest", opentracing.ChildOf(sCtx))
				} else {
					log.Warnf("%+v", errors.Wrap(err, "processObjTransferRequest, extract span info from request"))
				}
			}

			if span == nil {
				span = tracer.StartSpan("processObjTransferRequest")
			}

			defer span.Finish()
			ctx = opentracing.ContextWithSpan(ctx, span)
		}

		oid = header.GetObjectId()
		if len(oid) != 20 {
			finErr = errors.Errorf("oid len error: %v", len(oid))
			goto FIN
		}
		buff, err := server.PlasmaClient.GetBuff(ctx, oid)
		defer buff.Release(ctx)
		if err != nil {
			finErr = errors.Wrap(err, "PlasmaClient.GetBuff")
			goto FIN
		}
		if buff.IsEmpty() {
			finErr = errors.Errorf("object not found")
		} else {
			data = util.BytesWithoutCopy(buff.Data(), buff.Size())
		}
	}
FIN:
	err := func() error {
		var objectLength = -1
		if data != nil {
			objectLength = len(data)
		}
		errMsg := ""
		if finErr != nil {
			errMsg = finErr.Error()
		}

		rdmaSupport := server.RdmaDevice != nil && remoteRdmaSupport && objectLength >= int(server.RdmaTransferThreshold)
		header := &pb_object_fetch.FetchHeader{
			ObjectId:     oid,
			ObjectLength: int64(objectLength),
			RdmaSupport:  rdmaSupport,
			ErrorMsg:     errMsg,
		}
		buf, err := proto.Marshal(header)
		if err != nil {
			return errors.Wrap(err, "proto.Marshal")
		}

		_span, _ := opentracing.StartSpanFromContext(ctx, "processObjTransferRequest.WriteHeader")
		_, err = rw.Write(buf)
		_span.Finish()
		if err != nil {
			return errors.Wrap(err, "socket write")
		}

		if rdmaSupport {
			rdmaCtx, err := server.RdmaDevice.NewContext(ctx)
			if err != nil {
				return errors.Wrap(err, "RdmaDevice.NewContext")
			}
			defer rdmaCtx.Close(ctx)

			chRegMr := make(chan error, 1)
			go func() {
				chRegMr <- rdmaCtx.RegMr(ctx, data)
			}()

			connInfo, err := rdmaCtx.GetConnectionInfo()
			if err != nil {
				return errors.Wrap(err, "RdmaCtx.GetConnectionInfo")
			}
			connInfoMsg := &pb_object_fetch.ConnectionInfo{Info: connInfo.ToBytes()}
			buf, err := proto.Marshal(connInfoMsg)
			if err != nil {
				return errors.Wrap(err, "proto.Marshal")
			}
			_, err = rw.Write(buf)
			if err != nil {
				return errors.Wrap(err, "socket write")
			}

			buf, err = rw.Read(nil)
			if err != nil {
				return errors.Wrap(err, "socket read")
			}

			err = proto.Unmarshal(buf, connInfoMsg)
			if err != nil {
				return errors.Wrap(err, "proto.Unmarshal")
			}
			connInfo = rdma.ConnectionInfoFromBytes(connInfoMsg.GetInfo())
			err = rdmaCtx.Connect(ctx, connInfo)
			if err != nil {
				return errors.Wrap(err, "rdmaCtx.Connect")
			}

			err = <-chRegMr
			if err != nil {
				return errors.Wrap(err, "RegMr")
			}

			rbuf, err := rdmaCtx.GetBuf()
			if err != nil {
				return errors.Wrap(err, "RdmaCtx.GetBuf")
			}
			err = rdmaCtx.PostRecvEmpty(ctx)
			if err != nil {
				return errors.Wrap(err, "RdmaCtx.PostRecvEmpty")
			}

			rBufMsg := pb_object_fetch.RBuf{Rbuf: rbuf.ToBytes()}
			buf, err = proto.Marshal(&rBufMsg)
			if err != nil {
				return errors.Wrap(err, "proto.Marshal")
			}
			_, err = rw.Write(buf)
			if err != nil {
				return errors.Wrap(err, "socket write")
			}
			err = rdmaCtx.Poll(ctx, rdma.POLL_TIMEOUT_MS)
			if err != nil {
				return errors.Wrap(err, "RdmaCtx.Poll")
			}
		} else {
			_span, _ = opentracing.StartSpanFromContext(ctx, "processObjTransferRequest.WriteData")
			_, err = rw.Write(data)
			_span.Finish()
			if err != nil {
				return errors.Wrap(err, "socket write data")
			}
		}

		if span != nil {
			span.LogFields(ot_log.Int("object_length", len(data)))
		}

		return nil
	}()
	if finErr != nil {
		log.Error(errors.Errorf("processObjTransferRequest error: %+v", finErr))
	}
	if err != nil {
		log.Error(errors.Errorf("processObjTransferRequest error: %+v", err))
	}
	log.Debugf("processObjTransferRequest end, remote: %v", conn.RemoteAddr().String())
}

func (server *StorageServer) processObjTransferRequestTmp(ctx context.Context, conn net.Conn) {
	rw := util.NewBytesReadWriter(conn)
	defer conn.Close()

	var oid, data []byte
	var finErr error
	var span opentracing.Span

	{
		buf, err := rw.Read(nil)
		if err != nil {
			finErr = err
			goto FIN
		}

		header := fb_object.BytesToFetchTaskHeader(buf)
		if err := header.ErrorBytes(); len(err) > 0 {
			finErr = errors.New(string(err))
			goto FIN
		}

		if tracing.Enabled {
			tracer := opentracing.GlobalTracer()
			if spanCtxBytes := header.SpanContextBytes(); len(spanCtxBytes) > 0 {
				carrier := bytes.NewReader(spanCtxBytes)
				if sCtx, err := tracer.Extract(opentracing.Binary, carrier); err == nil {
					span = tracer.StartSpan("processObjTransferRequest", opentracing.ChildOf(sCtx))
				} else {
					log.Warnf("%+v", errors.Wrap(err, "processObjTransferRequest, extract span info from request"))
				}
			}

			if span == nil {
				span = tracer.StartSpan("processObjTransferRequest")
			}

			defer span.Finish()
			ctx = opentracing.ContextWithSpan(ctx, span)
		}

		meta := header.ObjectMeta(nil)
		oid = meta.OidBytes()
		if len(oid) != 20 {
			finErr = fmt.Errorf("oid len error: %v", len(oid))
			goto FIN
		}
		buff, err := server.PlasmaClient.GetBuff(ctx, oid)
		defer buff.Release(ctx)
		if err != nil {
			finErr = err
			goto FIN
		}
		if !buff.IsEmpty() {
			data = util.BytesWithoutCopy(buff.Data(), buff.Size())
		}
	}
FIN:
	err := func() error {
		var errBytes []byte
		var objectLength = -1
		if finErr != nil {
			errBytes = []byte(finErr.Error())
		} else {
			if data != nil {
				objectLength = len(data)
			}
		}

		// todo create_time, version
		buf := fb_object.FetchTaskHeaderToBytes(oid, int64(objectLength), -1, -1, nil, errBytes)
		_span, _ := opentracing.StartSpanFromContext(ctx, "processObjTransferRequest.WriteMeta")
		_, err := rw.Write(buf)
		_span.Finish()
		if err != nil {
			return err
		}
		_span, _ = opentracing.StartSpanFromContext(ctx, "processObjTransferRequest.WriteData")
		_, err = rw.Write(data)
		_span.Finish()

		if span != nil {
			span.LogFields(ot_log.Int("object_length", len(data)))
		}

		return err
	}()
	if err != nil {
		log.Error(fmt.Errorf("processObjTransferRequest error: %v", err))
	}
}

func (server *StorageServer) fetchViaRpc(ctx context.Context, oid []byte, srcNode string, srcNodeOnly bool) error {
	return server.fetchWithRetry(ctx, oid, srcNode, srcNodeOnly, server.tryFetchViaRpc)
}

func (server *StorageServer) tryFetchViaRpc(ctx context.Context, oid []byte, remoteServerInfo *pb_node_tracker.StorageServer) error {
	target := remoteServerInfo.GetRpcTarget()

	span, _ := opentracing.StartSpanFromContext(ctx, "grpc.Dial")
	conn, err := grpc.DialContext(ctx, target, util.GrpcDialOption()...)

	if err != nil {
		span.LogFields(ot_log.Error(err))
		span.Finish()
		return errors.Wrapf(err, "grpc.Dial target: %v", target)
	}
	span.Finish()

	client := pbObjectStore.NewObjectStoreClient(conn)
	resp, err := client.Get(ctx, &pbObjectStore.GetRequest{ObjectIdHex: hex.EncodeToString(oid)})
	if err != nil {
		return errors.Wrapf(err, "ObjectStoreClient.Get")
	}
	if resp.GetNotFound() {
		return errors.Wrapf(util.ErrNotFound, "object %v not found, target %v", oid, target)
	}

	data := resp.GetData()

	return server.PlasmaClient.Put(ctx, oid, data)
}
