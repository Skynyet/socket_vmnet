// Unit tests for the framed-stream ingress in main.c: read_exact() and
// read_frame(). main.c is included directly so the static functions are
// reachable; its main() is renamed to avoid a symbol clash.
#define main socket_vmnet_main
#include "../main.c"
#undef main

#include <sys/socket.h>

static int failures = 0;


#define CHECK(cond, name)                                                      \
  do {                                                                         \
    if (cond) {                                                                \
      printf("ok: %s\n", name);                                                \
    } else {                                                                   \
      fprintf(stderr, "FAIL: %s (line %d)\n", name, __LINE__);                 \
      failures++;                                                              \
    }                                                                          \
  } while (0)

static void write_all(int fd, const void *buf, size_t len) {
  const uint8_t *p = buf;
  size_t off = 0;
  while (off < len) {
    ssize_t written = write(fd, p + off, len - off);
    if (written <= 0) {
      perror("write");
      exit(2);
    }
    off += (size_t)written;
  }
}

static void write_header(int fd, uint32_t len) {
  uint32_t be = htonl(len);
  write_all(fd, &be, sizeof(be));
}

// Each check gets a fresh connected socketpair. Index 0 is read by the code
// under test, index 1 is written by the test.
static void socket_pair(int fds[2]) {
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
    perror("socketpair");
    exit(2);
  }
}

static void test_complete_frame_in_one_write(void) {
  int fds[2];
  socket_pair(fds);
  uint8_t payload[] = {0xde, 0xad, 0xbe, 0xef, 0x01};
  size_t len = sizeof(payload);
  uint32_t be = htonl((uint32_t)len);
  uint8_t framed[sizeof(be) + sizeof(payload)];
  memcpy(framed, &be, sizeof(be));
  memcpy(framed + sizeof(be), payload, sizeof(payload));
  write_all(fds[1], framed, sizeof(framed));

  uint8_t buf[64] = {0};
  size_t frame_len = 0;
  frame_read_status_t status = read_frame(fds[0], buf, sizeof(buf), &frame_len);
  CHECK(status == FRAME_READ_OK, "complete frame in one write -> OK");
  CHECK(frame_len == len, "complete frame length matches header");
  CHECK(memcmp(buf, payload, len) == 0, "complete frame body matches payload");
  close(fds[0]);
  close(fds[1]);
}

static void test_header_and_body_split_writes(void) {
  int fds[2];
  socket_pair(fds);
  uint8_t payload[] = {0x11, 0x22, 0x33};
  write_header(fds[1], (uint32_t)sizeof(payload)); // header alone in one write
  write_all(fds[1], payload, sizeof(payload));     // body alone in a second write
  uint8_t buf[64] = {0};
  size_t frame_len = 0;
  frame_read_status_t status = read_frame(fds[0], buf, sizeof(buf), &frame_len);
  CHECK(status == FRAME_READ_OK, "header and body split across writes -> OK");
  CHECK(frame_len == sizeof(payload), "split frame length matches header");
  CHECK(memcmp(buf, payload, sizeof(payload)) == 0, "split frame body matches payload");
  close(fds[0]);
  close(fds[1]);
}

static void test_body_in_single_byte_writes(void) {
  int fds[2];
  socket_pair(fds);
  uint8_t payload[] = {0x9a, 0x9b, 0x9c, 0x9d, 0x9e};
  write_header(fds[1], (uint32_t)sizeof(payload));
  for (size_t i = 0; i < sizeof(payload); i++)
    write_all(fds[1], &payload[i], 1);
  uint8_t buf[64] = {0};
  size_t frame_len = 0;
  frame_read_status_t status = read_frame(fds[0], buf, sizeof(buf), &frame_len);
  CHECK(status == FRAME_READ_OK, "body delivered one byte at a time -> OK");
  CHECK(frame_len == sizeof(payload), "byte-wise body length matches header");
  CHECK(memcmp(buf, payload, sizeof(payload)) == 0, "byte-wise body matches payload");
  close(fds[0]);
  close(fds[1]);
}

static void test_eof_between_frames(void) {
  int fds[2];
  socket_pair(fds);
  close(fds[1]); // clean close before anything is sent
  uint8_t buf[64];
  size_t frame_len = 0;
  frame_read_status_t status = read_frame(fds[0], buf, sizeof(buf), &frame_len);
  CHECK(status == FRAME_READ_EOF, "clean close before header -> EOF");
  close(fds[0]);
}

static void test_truncated_header(void) {
  int fds[2];
  socket_pair(fds);
  uint8_t two[2] = {0x00, 0x00};
  write_all(fds[1], two, sizeof(two));
  close(fds[1]);
  uint8_t buf[64];
  size_t frame_len = 0;
  frame_read_status_t status = read_frame(fds[0], buf, sizeof(buf), &frame_len);
  CHECK(status == FRAME_READ_TRUNCATED, "two-byte header then close -> TRUNCATED");
  close(fds[0]);
}

static void test_truncated_header_with_pending_body(void) {
  // Close mid-header while body bytes of a next frame are already queued:
  // the peer is at fault and the stream must not silently continue.
  int fds[2];
  socket_pair(fds);
  uint8_t two[2] = {0xaa, 0xbb};
  write_all(fds[1], two, sizeof(two)); // only 2 of 4 header bytes
  close(fds[1]);
  uint32_t header_be = 0;
  ssize_t header_received = read_exact(fds[0], &header_be, sizeof(header_be));
  CHECK(header_received == 2, "partial header then close -> short read");
  close(fds[0]);
}

static void test_truncated_body(void) {
  int fds[2];
  socket_pair(fds);
  write_header(fds[1], 10);
  uint8_t three[3] = {0x01, 0x02, 0x03};
  write_all(fds[1], three, sizeof(three));
  close(fds[1]);
  uint8_t buf[64];
  size_t frame_len = 0;
  frame_read_status_t status = read_frame(fds[0], buf, sizeof(buf), &frame_len);
  CHECK(status == FRAME_READ_TRUNCATED, "3 of 10 body bytes then close -> TRUNCATED");
  close(fds[0]);
}

static void test_zero_length_frame_rejected(void) {
  int fds[2];
  socket_pair(fds);
  write_header(fds[1], 0);
  close(fds[1]);
  uint8_t buf[64];
  size_t frame_len = 0;
  frame_read_status_t status = read_frame(fds[0], buf, sizeof(buf), &frame_len);
  CHECK(status == FRAME_READ_INVALID, "zero-length header -> INVALID");
  close(fds[0]);
}

static void test_oversized_frame_rejected(void) {
  int fds[2];
  socket_pair(fds);
  uint8_t buf[16];
  write_header(fds[1], (uint32_t)(sizeof(buf) + 1));
  close(fds[1]);
  size_t frame_len = 0;
  frame_read_status_t status = read_frame(fds[0], buf, sizeof(buf), &frame_len);
  CHECK(status == FRAME_READ_INVALID, "header larger than buffer -> INVALID");
  close(fds[0]);
}

static void eintr_handler(int sig) {
  (void)sig;
}

static void test_eintr_is_retried(void) {
  int fds[2];
  socket_pair(fds);
  struct sigaction sa = {0};
  sa.sa_handler = eintr_handler; // no SA_RESTART: read() must fail with EINTR
  sigemptyset(&sa.sa_mask);
  if (sigaction(SIGALRM, &sa, NULL) != 0) {
    perror("sigaction");
    exit(2);
  }
  alarm(1); // fires while read_exact() is blocked, before the peer writes
  uint8_t payload[] = {0x42};
  write_header(fds[1], (uint32_t)sizeof(payload));
  write_all(fds[1], payload, sizeof(payload));
  uint8_t buf[64] = {0};
  size_t frame_len = 0;
  frame_read_status_t status = read_frame(fds[0], buf, sizeof(buf), &frame_len);
  alarm(0);
  CHECK(status == FRAME_READ_OK, "EINTR during read -> retried, frame still OK");
  CHECK(frame_len == sizeof(payload) && buf[0] == payload[0], "EINTR frame body intact");
  close(fds[0]);
  close(fds[1]);
}

static void test_buffer_reuse_across_frames(void) {
  int fds[2];
  socket_pair(fds);
  uint8_t first[] = {0x01, 0x02};
  uint8_t second[] = {0xf1, 0xf2, 0xf3, 0xf4};
  write_header(fds[1], (uint32_t)sizeof(first));
  write_all(fds[1], first, sizeof(first));
  write_header(fds[1], (uint32_t)sizeof(second));
  write_all(fds[1], second, sizeof(second));
  uint8_t buf[64] = {0};
  size_t frame_len = 0;
  CHECK(read_frame(fds[0], buf, sizeof(buf), &frame_len) == FRAME_READ_OK &&
            frame_len == sizeof(first),
        "first frame of two -> OK");
  CHECK(read_frame(fds[0], buf, sizeof(buf), &frame_len) == FRAME_READ_OK &&
            frame_len == sizeof(second) && memcmp(buf, second, sizeof(second)) == 0,
        "second frame of two -> OK");
  close(fds[0]);
  close(fds[1]);
}

static void test_existing_non_socket_is_not_unlinked(void) {
  errno = 0;
  CHECK(legacy_socket_path_preflight("/dev/null") < 0 && errno == EADDRINUSE,
        "existing non-socket path rejected before bus startup");
}

int main(void) {
  test_complete_frame_in_one_write();
  test_header_and_body_split_writes();
  test_body_in_single_byte_writes();
  test_eof_between_frames();
  test_truncated_header();
  test_truncated_header_with_pending_body();
  test_truncated_body();
  test_zero_length_frame_rejected();
  test_oversized_frame_rejected();
  test_eintr_is_retried();
  test_buffer_reuse_across_frames();
  test_existing_non_socket_is_not_unlinked();
  if (failures > 0) {
    fprintf(stderr, "%d failure(s)\n", failures);
    return 1;
  }
  printf("all frame_read tests passed\n");
  return 0;
}
