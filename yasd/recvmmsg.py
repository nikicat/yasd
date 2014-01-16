import cffi

def recv_mmsg(stream, sock, vlen=1000, bufsize=9000):
    ffi = cffi.FFI()
    ffi.cdef('''
        typedef unsigned int socklen_t;
        struct msghdr {
            void *msg_name;/* Address to send to/receive from.  */
            socklen_t msg_namelen;/* Length of address data.  */

            struct iovec *msg_iov;/* Vector of data to send/receive into.  */
            size_t msg_iovlen;/* Number of elements in the vector.  */

            void *msg_control;/* Ancillary data (eg BSD filedesc passing). */
            size_t msg_controllen;/* Ancillary data buffer length.
                                   !! The type should be socklen_t but the
                                   definition of the kernel is incompatible
                                   with this.  */

            int msg_flags;/* Flags on received message.  */
        };
        struct mmsghdr {
            struct msghdr msg_hdr;  /* Message header */
            unsigned int  msg_len;  /* Number of received bytes for header */
        };
        struct iovec {
            void *iov_base;
            size_t iov_len;
        };
        int recvmmsg(int sockfd, struct mmsghdr *msgvec, unsigned int vlen,unsigned int flags, struct timespec *timeout);
    ''')
    lib = ffi.verify('''
        #include <sys/socket.h>
    ''', libraries=[])
    MSG_WAITFORONE = 0x10000
    msgs = ffi.new('struct mmsghdr[{}]'.format(vlen))
    iovecs = ffi.new('struct iovec[{}]'.format(vlen))
    bufs = ffi.new('char[{}][{}]'.format(vlen, bufsize))
    for i, iovec in enumerate(iovecs):
        iovec.iov_base = bufs[i]
        iovec.iov_len = bufsize
        msgs[i].msg_hdr.msg_iov = ffi.addressof(iovec)
        msgs[i].msg_hdr.msg_iovlen = 1

    timeout = ffi.cast('struct timespec *', 0)
    fd = sock.fileno()

    for _ in stream:
        retval = lib.recvmmsg(fd, msgs, vlen, MSG_WAITFORONE, timeout)
        if retval == -1:
            raise Exception('recvmmsg failed')
        for i in range(retval):
            yield ffi.string(bufs[i], msgs[i].msg_len)
