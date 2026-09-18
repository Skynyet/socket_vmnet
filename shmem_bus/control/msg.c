// Message and capability transport for the SOCK_STREAM control lease. Reads
// assemble fixed-size messages and retain attached descriptors until complete.
#include "../include/bus_control.h"

#include <errno.h>
#include <poll.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static int64_t msg_now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int wait_ready(int sock, short events, int64_t deadline_ms) {
  int64_t left = deadline_ms - msg_now_ms();
  if (left <= 0) {
    errno = ETIMEDOUT;
    return -1;
  }
  struct pollfd pf = {.fd = sock, .events = events, .revents = 0};
  int r = poll(&pf, 1, (int)left);
  if (r == 0) {
    errno = ETIMEDOUT;
    return -1;
  }
  return (r < 0) ? -1 : 0;
}

// The descriptor goes with the first sendmsg that moves any bytes. If the
// socket buffer fills we wait, but never forever: a peer that will not drain
// its control stream is a peer to give up on, and the caller drains it.
int bus_msg_send(int sock, const struct bus_msg *m, int fd) {
  const unsigned char *p = (const unsigned char *)m;
  size_t left = sizeof(*m);
  int carrying = (fd >= 0);
  int64_t deadline = msg_now_ms() + BUS_MSG_SEND_TIMEOUT_MS;

  while (left > 0) {
    struct iovec io = {.iov_base = (void *)(uintptr_t)p, .iov_len = left};
    char cbuf[CMSG_SPACE(sizeof(int))];
    memset(cbuf, 0, sizeof(cbuf));
    struct msghdr h = {.msg_iov = &io, .msg_iovlen = 1};
    if (carrying) {
      h.msg_control = cbuf;
      h.msg_controllen = sizeof(cbuf);
      struct cmsghdr *c = CMSG_FIRSTHDR(&h);
      c->cmsg_level = SOL_SOCKET;
      c->cmsg_type = SCM_RIGHTS;
      c->cmsg_len = CMSG_LEN(sizeof(int));
      memcpy(CMSG_DATA(c), &fd, sizeof(int));
    }
    ssize_t n;
    do {
      n = sendmsg(sock, &h, 0);
    } while (n < 0 && errno == EINTR);
    if (n < 0) {
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        return -1;
      }
      if (wait_ready(sock, POLLOUT, deadline) < 0) {
        return -1;
      }
      continue;
    }
    if (n > 0) {
      carrying = 0;  // it went with those bytes; sending it twice would duplicate it
      p += n;
      left -= (size_t)n;
    }
  }
  return 0;
}

void bus_msg_reader_init(struct bus_msg_reader *r) {
  memset(r, 0, sizeof(*r));
  r->fd = -1;
}

void bus_msg_reader_close(struct bus_msg_reader *r) {
  if (r->fd >= 0) {
    close(r->fd);
    r->fd = -1;
  }
  r->got = 0;
}

// Returns 1 on a whole message, 0 on orderly EOF -- which is the lease ending,
// not an error -- BUS_MSG_INCOMPLETE when more is needed and the caller should
// wait for readability again, and -1 on failure. *fd is -1 when the message
// carried no capability.
int bus_msg_read(int sock, struct bus_msg_reader *r, struct bus_msg *m, int *fd) {
  *fd = -1;
  while (r->got < sizeof(r->partial)) {
    struct iovec io = {.iov_base = (unsigned char *)&r->partial + r->got,
                       .iov_len = sizeof(r->partial) - r->got};
    char cbuf[CMSG_SPACE(sizeof(int))];
    struct msghdr h = {.msg_iov = &io, .msg_iovlen = 1, .msg_control = cbuf,
                       .msg_controllen = sizeof(cbuf)};
    ssize_t n;
    do {
      n = recvmsg(sock, &h, 0);
    } while (n < 0 && errno == EINTR);
    if (n == 0) {
      return 0;
    }
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return BUS_MSG_INCOMPLETE;
      }
      return -1;
    }
    // MSG_CTRUNC means the kernel dropped a descriptor we have no way to
    // recover. Failing here is the only safe answer: continuing would leave the
    // peer believing it holds a capability it does not.
    if ((h.msg_flags & MSG_CTRUNC) != 0) {
      errno = EPROTO;
      return -1;
    }
    for (struct cmsghdr *c = CMSG_FIRSTHDR(&h); c != NULL; c = CMSG_NXTHDR(&h, c)) {
      if (c->cmsg_level != SOL_SOCKET || c->cmsg_type != SCM_RIGHTS ||
          c->cmsg_len != CMSG_LEN(sizeof(int))) {
        continue;
      }
      int got = -1;
      memcpy(&got, CMSG_DATA(c), sizeof(int));
      if (r->fd >= 0) {
        // Two descriptors inside one message is not this protocol. Keep the
        // first so the caller's cleanup finds it, and refuse.
        close(got);
        errno = EPROTO;
        return -1;
      }
      r->fd = got;
    }
    r->got += (size_t)n;
  }
  *m = r->partial;
  *fd = r->fd;
  r->fd = -1;
  r->got = 0;
  return 1;
}

// For the one exchange that is not event-driven: the HELLO that decides whether
// a connection becomes a port at all. Bounded, so a connection that says
// nothing costs one deadline and not the coordinator.
int bus_msg_read_by(int sock, struct bus_msg_reader *r, struct bus_msg *m, int *fd,
                    int64_t timeout_ms) {
  int64_t deadline = msg_now_ms() + timeout_ms;
  for (;;) {
    int n = bus_msg_read(sock, r, m, fd);
    if (n != BUS_MSG_INCOMPLETE) {
      return n;
    }
    if (wait_ready(sock, POLLIN, deadline) < 0) {
      return -1;
    }
  }
}
