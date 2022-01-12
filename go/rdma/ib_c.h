#ifndef _IB_C_H_
#define _IB_C_H_
#include <stdint.h>
#include <infiniband/verbs.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C"
{
#endif
    typedef void *IBDevicePtr;
    typedef void *IBContextPtr;

    typedef struct
    {
        union ibv_gid gid;
        uint32_t qp_num;
        uint16_t lid;
    } IBConnectionInfo;

    typedef struct
    {
        void *addr;
        uint64_t length;
        uint32_t lkey;
        uint32_t rkey;
    } IBBuf;

    const char *IBOpenDevice(char *dev_name, uint8_t ib_port, uint8_t gid_idx, IBDevicePtr *device);
    const char *IBCloseDevice(IBDevicePtr device);

    const char *IBNewContext(IBDevicePtr device, IBContextPtr *context);
    const char *IBCloseContext(IBContextPtr context);

    const char *IBRegMr(IBContextPtr context, void *buf, int64_t size);
    const char *IBUnRegMr(IBContextPtr context);
    const char *IBConnect(IBContextPtr context, IBConnectionInfo *info);
    const char *IBPostRead(IBContextPtr context, void *laddr, void *raddr, uint32_t length, uint32_t rkey, bool send_signaled);
    const char *IBPostSendEmpty(IBContextPtr context, bool send_signaled);
    const char *IBPostRecvEmpty(IBContextPtr context);
    const char *IBPoll(IBContextPtr context, int timeout_ms);
    const char *IBGetConnectionInfo(IBContextPtr context, IBConnectionInfo *info);
    const char *IBGetBuf(IBContextPtr context, IBBuf *buf);

#ifdef __cplusplus
}
#endif // extern "C"

#endif // #ifdef _IB_C_H_
