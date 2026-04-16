/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sched.h>
#include <dirent.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 65536
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED,
    CONTAINER_HARD_LIMIT_KILLED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    int run_client_fd;
    int log_read_fd;
    int producer_started;
    pthread_t producer_thread;
    void *child_stack;
    char rootfs[PATH_MAX];
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    char log_path[PATH_MAX];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    volatile sig_atomic_t should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    char container_id[CONTAINER_ID_LEN];
    char log_path[PATH_MAX];
    int fd;
} producer_arg_t;

static supervisor_ctx_t *g_ctx;
static volatile sig_atomic_t g_run_stop_signal;

static void copy_string(char *dst, size_t dst_size, const char *src)
{
    if (dst_size == 0)
        return;
    snprintf(dst, dst_size, "%s", src ? src : "");
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    case CONTAINER_HARD_LIMIT_KILLED:
        return "hard_limit_killed";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return 0;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 1;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) > 0) {
        int fd = open(item.log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0)
            continue;

        size_t written = 0;
        while (written < item.length) {
            ssize_t n = write(fd, item.data + written, item.length - written);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                break;
            }
            written += (size_t)n;
        }
        close(fd);
    }

    return NULL;
}

static void *producer_thread(void *arg)
{
    producer_arg_t *pa = arg;
    char buf[LOG_CHUNK_SIZE];

    for (;;) {
        ssize_t n = read(pa->fd, buf, sizeof(buf));
        if (n < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (n == 0)
            break;

        log_item_t item;
        memset(&item, 0, sizeof(item));
        copy_string(item.container_id, sizeof(item.container_id), pa->container_id);
        copy_string(item.log_path, sizeof(item.log_path), pa->log_path);
        memcpy(item.data, buf, (size_t)n);
        item.length = (size_t)n;

        if (bounded_buffer_push(&pa->ctx->log_buffer, &item) != 0)
            break;
    }

    close(pa->fd);
    free(pa);
    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = arg;

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 1;
    }
    close(cfg->log_write_fd);

    if (cfg->nice_value != 0 && setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0)
        perror("setpriority");

    if (sethostname(cfg->id, strlen(cfg->id)) < 0)
        perror("sethostname");

    if (chdir(cfg->rootfs) < 0) {
        perror("chdir rootfs");
        return 1;
    }
    if (chroot(".") < 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") < 0) {
        perror("chdir /");
        return 1;
    }

    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) < 0)
        perror("mount /proc");

    execl("/bin/sh", "sh", "-c", cfg->command, (char *)NULL);
    perror("exec /bin/sh");
    return 127;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    copy_string(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    copy_string(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static void signal_supervisor(int sig)
{
    if (g_ctx)
        g_ctx->should_stop = 1;
    (void)sig;
}

static void signal_child_changed(int sig)
{
    (void)sig;
}

static void signal_run_client_stop(int sig)
{
    (void)sig;
    g_run_stop_signal = 1;
}

static int send_stop_intent(const char *container_id)
{
    int fd;
    struct sockaddr_un addr;
    control_request_t req;
    control_response_t resp;
    size_t off = 0;
    const char *p;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_string(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    copy_string(req.container_id, sizeof(req.container_id), container_id);

    p = (const char *)&req;
    while (off < sizeof(req)) {
        ssize_t n = write(fd, p + off, sizeof(req) - off);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            close(fd);
            return -1;
        }
        off += (size_t)n;
    }

    off = 0;
    p = (const char *)&resp;
    while (off < sizeof(resp)) {
        ssize_t n = read(fd, (char *)p + off, sizeof(resp) - off);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (n == 0)
            break;
        off += (size_t)n;
    }

    close(fd);
    return 0;
}

static container_record_t *find_container_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *cur;

    for (cur = ctx->containers; cur; cur = cur->next) {
        if (strncmp(cur->id, id, sizeof(cur->id)) == 0)
            return cur;
    }
    return NULL;
}

static int rootfs_in_use_locked(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *cur;

    for (cur = ctx->containers; cur; cur = cur->next) {
        if ((cur->state == CONTAINER_STARTING || cur->state == CONTAINER_RUNNING) &&
            strcmp(cur->rootfs, rootfs) == 0)
            return 1;
    }
    return 0;
}

static void set_response(control_response_t *resp, int status, const char *fmt, ...)
{
    va_list ap;

    resp->status = status;
    va_start(ap, fmt);
    vsnprintf(resp->message, sizeof(resp->message), fmt, ap);
    va_end(ap);
}

static int send_response_fd(int fd, const control_response_t *resp)
{
    size_t off = 0;
    const char *p = (const char *)resp;

    while (off < sizeof(*resp)) {
        ssize_t n = write(fd, p + off, sizeof(*resp) - off);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        off += (size_t)n;
    }
    return 0;
}

static int recv_request_fd(int fd, control_request_t *req)
{
    size_t off = 0;
    char *p = (char *)req;

    while (off < sizeof(*req)) {
        ssize_t n = read(fd, p + off, sizeof(*req) - off);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        off += (size_t)n;
    }
    return 0;
}

static int start_container(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           int run_client_fd,
                           control_response_t *resp)
{
    int pipefd[2] = {-1, -1};
    child_config_t *cfg = NULL;
    container_record_t *rec = NULL;
    producer_arg_t *pa = NULL;
    void *stack = NULL;
    pid_t pid;
    char log_path[PATH_MAX];

    if (mkdir(LOG_DIR, 0755) < 0 && errno != EEXIST) {
        set_response(resp, 1, "mkdir %s failed: %s\n", LOG_DIR, strerror(errno));
        return -1;
    }

    snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, req->container_id);

    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_locked(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        set_response(resp, 1, "container %s already exists\n", req->container_id);
        return -1;
    }
    if (rootfs_in_use_locked(ctx, req->rootfs)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        set_response(resp, 1, "rootfs already used by a running container: %s\n", req->rootfs);
        return -1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pipe(pipefd) < 0) {
        set_response(resp, 1, "pipe failed: %s\n", strerror(errno));
        return -1;
    }

    cfg = calloc(1, sizeof(*cfg));
    rec = calloc(1, sizeof(*rec));
    pa = calloc(1, sizeof(*pa));
    stack = malloc(STACK_SIZE);
    if (!cfg || !rec || !pa || !stack) {
        set_response(resp, 1, "allocation failed\n");
        goto fail;
    }

    copy_string(cfg->id, sizeof(cfg->id), req->container_id);
    copy_string(cfg->rootfs, sizeof(cfg->rootfs), req->rootfs);
    copy_string(cfg->command, sizeof(cfg->command), req->command);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = pipefd[1];

    pid = clone(child_fn, (char *)stack + STACK_SIZE,
                CLONE_NEWUTS | CLONE_NEWPID | CLONE_NEWNS | SIGCHLD, cfg);
    if (pid < 0) {
        set_response(resp, 1, "clone failed: %s\n", strerror(errno));
        goto fail;
    }
    close(pipefd[1]);
    pipefd[1] = -1;
    free(cfg);
    cfg = NULL;

    pa->ctx = ctx;
    pa->fd = pipefd[0];
    copy_string(pa->container_id, sizeof(pa->container_id), req->container_id);
    copy_string(pa->log_path, sizeof(pa->log_path), log_path);

    copy_string(rec->id, sizeof(rec->id), req->container_id);
    copy_string(rec->rootfs, sizeof(rec->rootfs), req->rootfs);
    copy_string(rec->log_path, sizeof(rec->log_path), log_path);
    rec->host_pid = pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->exit_code = -1;
    rec->exit_signal = 0;
    rec->run_client_fd = run_client_fd;
    rec->log_read_fd = pipefd[0];
    rec->child_stack = stack;

    if (pthread_create(&rec->producer_thread, NULL, producer_thread, pa) != 0) {
        kill(pid, SIGKILL);
        set_response(resp, 1, "pthread_create producer failed\n");
        pa = NULL;
        goto fail;
    }
    rec->producer_started = 1;
    pipefd[0] = -1;
    pa = NULL;

    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0 &&
        register_with_monitor(ctx->monitor_fd, rec->id, rec->host_pid,
                              rec->soft_limit_bytes, rec->hard_limit_bytes) < 0) {
        fprintf(stderr, "warning: monitor register failed for %s: %s\n",
                rec->id, strerror(errno));
    }

    set_response(resp, 0, "started %s pid=%d log=%s\n", rec->id, rec->host_pid, rec->log_path);
    return 0;

fail:
    if (pipefd[0] >= 0)
        close(pipefd[0]);
    if (pipefd[1] >= 0)
        close(pipefd[1]);
    free(cfg);
    free(rec);
    free(pa);
    free(stack);
    return -1;
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        container_record_t *rec = NULL;
        control_response_t resp;

        pthread_mutex_lock(&ctx->metadata_lock);
        for (rec = ctx->containers; rec; rec = rec->next) {
            if (rec->host_pid == pid)
                break;
        }
        if (rec) {
            if (WIFEXITED(status)) {
                rec->exit_code = WEXITSTATUS(status);
                rec->state = rec->stop_requested ? CONTAINER_STOPPED : CONTAINER_EXITED;
            } else if (WIFSIGNALED(status)) {
                rec->exit_signal = WTERMSIG(status);
                if (rec->stop_requested)
                    rec->state = CONTAINER_STOPPED;
                else if (rec->exit_signal == SIGKILL)
                    rec->state = CONTAINER_HARD_LIMIT_KILLED;
                else
                    rec->state = CONTAINER_KILLED;
            }
            if (ctx->monitor_fd >= 0)
                unregister_from_monitor(ctx->monitor_fd, rec->id, rec->host_pid);

            if (rec->run_client_fd >= 0) {
                int code = rec->exit_signal ? 128 + rec->exit_signal : rec->exit_code;
                set_response(&resp, code, "%s finished state=%s exit=%d signal=%d\n",
                             rec->id, state_to_string(rec->state),
                             rec->exit_code, rec->exit_signal);
                send_response_fd(rec->run_client_fd, &resp);
                close(rec->run_client_fd);
                rec->run_client_fd = -1;
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;

    memset(&resp, 0, sizeof(resp));
    if (recv_request_fd(client_fd, &req) != 0) {
        close(client_fd);
        return;
    }

    if (req.kind == CMD_START || req.kind == CMD_RUN) {
        int keep_open = (req.kind == CMD_RUN);
        if (start_container(ctx, &req, keep_open ? client_fd : -1, &resp) == 0) {
            if (!keep_open) {
                send_response_fd(client_fd, &resp);
                close(client_fd);
            }
        } else {
            send_response_fd(client_fd, &resp);
            close(client_fd);
        }
        return;
    }

    if (req.kind == CMD_PS) {
        size_t used = 0;
        container_record_t *cur;
        pthread_mutex_lock(&ctx->metadata_lock);
        used += snprintf(resp.message + used, sizeof(resp.message) - used,
                         "%-16s %-8s %-8s %-8s %-8s %s\n",
                         "ID", "PID", "STATE", "SOFT", "HARD", "LOG");
        for (cur = ctx->containers; cur && used < sizeof(resp.message); cur = cur->next) {
            used += snprintf(resp.message + used, sizeof(resp.message) - used,
                             "%-16s %-8d %-8s %-8lu %-8lu %s\n",
                             cur->id, cur->host_pid, state_to_string(cur->state),
                             cur->soft_limit_bytes >> 20,
                             cur->hard_limit_bytes >> 20,
                             cur->log_path);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp.status = 0;
        send_response_fd(client_fd, &resp);
        close(client_fd);
        return;
    }

    if (req.kind == CMD_LOGS) {
        container_record_t *rec;
        int fd;
        ssize_t n;
        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_locked(ctx, req.container_id);
        if (!rec) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            set_response(&resp, 1, "unknown container %s\n", req.container_id);
        } else {
            char path[PATH_MAX];
            copy_string(path, sizeof(path), rec->log_path);
            pthread_mutex_unlock(&ctx->metadata_lock);
            fd = open(path, O_RDONLY);
            if (fd < 0) {
                set_response(&resp, 1, "cannot open %s: %s\n", path, strerror(errno));
            } else {
                n = read(fd, resp.message, sizeof(resp.message) - 1);
                if (n < 0)
                    set_response(&resp, 1, "read log failed: %s\n", strerror(errno));
                else {
                    resp.message[n] = '\0';
                    resp.status = 0;
                }
                close(fd);
            }
        }
        send_response_fd(client_fd, &resp);
        close(client_fd);
        return;
    }

    if (req.kind == CMD_STOP) {
        container_record_t *rec;
        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_locked(ctx, req.container_id);
        if (!rec) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            set_response(&resp, 1, "unknown container %s\n", req.container_id);
        } else if (rec->state != CONTAINER_RUNNING && rec->state != CONTAINER_STARTING) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            set_response(&resp, 1, "%s is not running\n", req.container_id);
        } else {
            rec->stop_requested = 1;
            kill(rec->host_pid, SIGTERM);
            pthread_mutex_unlock(&ctx->metadata_lock);
            set_response(&resp, 0, "sent SIGTERM to %s\n", req.container_id);
        }
        send_response_fd(client_fd, &resp);
        close(client_fd);
        return;
    }

    set_response(&resp, 1, "unknown command\n");
    send_response_fd(client_fd, &resp);
    close(client_fd);
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;
    struct sockaddr_un addr;
    struct sigaction sa;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "warning: /dev/container_monitor unavailable: %s\n", strerror(errno));

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    unlink(CONTROL_PATH);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_string(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(ctx.server_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }
    if (listen(ctx.server_fd, 16) < 0) {
        perror("listen");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_supervisor;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sa.sa_handler = signal_child_changed;
    sigaction(SIGCHLD, &sa, NULL);

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    printf("supervisor ready: base=%s socket=%s\n", rootfs, CONTROL_PATH);
    fflush(stdout);

    while (!ctx.should_stop) {
        fd_set rfds;
        struct timeval tv;
        int ready;

        reap_children(&ctx);

        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        ready = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (ready < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }
        if (ready > 0 && FD_ISSET(ctx.server_fd, &rfds)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd >= 0)
                handle_client(&ctx, client_fd);
        }
    }

    pthread_mutex_lock(&ctx.metadata_lock);
    for (container_record_t *cur = ctx.containers; cur; cur = cur->next) {
        if (cur->state == CONTAINER_RUNNING || cur->state == CONTAINER_STARTING) {
            cur->stop_requested = 1;
            kill(cur->host_pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    for (int i = 0; i < 5; i++) {
        reap_children(&ctx);
        sleep(1);
    }

    pthread_mutex_lock(&ctx.metadata_lock);
    for (container_record_t *cur = ctx.containers; cur; cur = cur->next) {
        if (cur->state == CONTAINER_RUNNING || cur->state == CONTAINER_STARTING)
            kill(cur->host_pid, SIGKILL);
    }
    pthread_mutex_unlock(&ctx.metadata_lock);
    reap_children(&ctx);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_mutex_lock(&ctx.metadata_lock);
    for (container_record_t *cur = ctx.containers; cur; cur = cur->next) {
        if (cur->producer_started)
            pthread_join(cur->producer_thread, NULL);
        free(cur->child_stack);
        if (cur->run_client_fd >= 0)
            close(cur->run_client_fd);
    }
    pthread_mutex_unlock(&ctx.metadata_lock);
    pthread_join(ctx.logger_thread, NULL);

    while (ctx.containers) {
        container_record_t *next = ctx.containers->next;
        free(ctx.containers);
        ctx.containers = next;
    }

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    close(ctx.server_fd);
    unlink(CONTROL_PATH);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    g_ctx = NULL;
    return 0;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;
    struct sigaction old_int;
    struct sigaction old_term;
    struct sigaction sa;
    int stop_sent = 0;
    int installed_run_handlers = 0;
    size_t off = 0;
    const char *p = (const char *)req;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_string(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "connect %s failed: %s\n", CONTROL_PATH, strerror(errno));
        close(fd);
        return 1;
    }

    if (req->kind == CMD_RUN) {
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = signal_run_client_stop;
        sigemptyset(&sa.sa_mask);
        g_run_stop_signal = 0;
        if (sigaction(SIGINT, &sa, &old_int) == 0 &&
            sigaction(SIGTERM, &sa, &old_term) == 0)
            installed_run_handlers = 1;
    }

    while (off < sizeof(*req)) {
        ssize_t n = write(fd, p + off, sizeof(*req) - off);
        if (n < 0) {
            if (errno == EINTR) {
                if (g_run_stop_signal && !stop_sent) {
                    send_stop_intent(req->container_id);
                    stop_sent = 1;
                }
                continue;
            }
            perror("write request");
            close(fd);
            if (installed_run_handlers) {
                sigaction(SIGINT, &old_int, NULL);
                sigaction(SIGTERM, &old_term, NULL);
            }
            return 1;
        }
        off += (size_t)n;
    }

    off = 0;
    p = (char *)&resp;
    while (off < sizeof(resp)) {
        ssize_t n = read(fd, (char *)p + off, sizeof(resp) - off);
        if (n < 0) {
            if (errno == EINTR) {
                if (g_run_stop_signal && !stop_sent) {
                    send_stop_intent(req->container_id);
                    stop_sent = 1;
                }
                continue;
            }
            perror("read response");
            close(fd);
            if (installed_run_handlers) {
                sigaction(SIGINT, &old_int, NULL);
                sigaction(SIGTERM, &old_term, NULL);
            }
            return 1;
        }
        if (n == 0) {
            fprintf(stderr, "supervisor closed connection\n");
            close(fd);
            if (installed_run_handlers) {
                sigaction(SIGINT, &old_int, NULL);
                sigaction(SIGTERM, &old_term, NULL);
            }
            return 1;
        }
        off += (size_t)n;
    }

    close(fd);
    if (installed_run_handlers) {
        sigaction(SIGINT, &old_int, NULL);
        sigaction(SIGTERM, &old_term, NULL);
    }
    fputs(resp.message, resp.status == 0 ? stdout : stderr);
    return resp.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);
    copy_string(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_string(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);
    copy_string(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_string(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}


