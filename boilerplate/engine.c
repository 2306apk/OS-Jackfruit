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
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
#define CONTROL_FIFO_PATH "/tmp/jackfruit_fifo"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

// forward declaration
void handle_signal(int sig);

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
    CONTAINER_EXITED
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
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
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
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
    void *stack;
    child_config_t *cfg;
} log_pipe_ctx_t;

static volatile sig_atomic_t g_shutdown_requested = 0;
static volatile sig_atomic_t g_reap_children_requested = 0;

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

static void copy_string(char *dst, size_t dst_sz, const char *src)
{
    if (dst_sz == 0)
        return;
    if (src == NULL)
        src = "";
    strncpy(dst, src, dst_sz - 1);
    dst[dst_sz - 1] = '\0';
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
 * Producer-side insertion into the bounded buffer.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    int rc = 0;

    if (buffer == NULL || item == NULL)
        return -1;

    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    if (buffer->shutting_down) {
        rc = -1;
        goto out;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);

out:
    pthread_mutex_unlock(&buffer->mutex);
    return rc;
}

/*
 * Consumer-side removal from the bounded buffer.
 * Returns 0 on success, 1 when shutdown is in progress and no item remains.
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    int rc = 0;

    if (buffer == NULL || item == NULL)
        return -1;

    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    if (buffer->count == 0 && buffer->shutting_down) {
        rc = 1;
        goto out;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);

out:
    pthread_mutex_unlock(&buffer->mutex);
    return rc;
}

static void ensure_logs_dir(void)
{
    if (mkdir(LOG_DIR, 0755) < 0 && errno != EEXIST) {
        perror("mkdir logs");
    }
}

static container_record_t *find_container_record_locked(container_record_t *head,
                                                        const char *id)
{
    container_record_t *cur;

    for (cur = head; cur != NULL; cur = cur->next) {
        if (strcmp(cur->id, id) == 0)
            return cur;
    }

    return NULL;
}

static container_record_t *find_container_by_pid_locked(container_record_t *head,
                                                       pid_t pid)
{
    container_record_t *cur;

    for (cur = head; cur != NULL; cur = cur->next) {
        if (cur->host_pid == pid)
            return cur;
    }

    return NULL;
}

static void add_container_record_locked(supervisor_ctx_t *ctx,
                                        const control_request_t *req,
                                        pid_t pid)
{
    container_record_t *rec;

    rec = calloc(1, sizeof(*rec));
    if (rec == NULL)
        return;

    copy_string(rec->id, sizeof(rec->id), req->container_id);
    rec->host_pid = pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->exit_code = 0;
    rec->exit_signal = 0;
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, req->container_id);

    rec->next = ctx->containers;
    ctx->containers = rec;
}

static void dump_containers(supervisor_ctx_t *ctx)
{
    container_record_t *cur;

    pthread_mutex_lock(&ctx->metadata_lock);
    printf("ID\tPID\tSTATE\tSOFT(MiB)\tHARD(MiB)\tSTARTED\n");
    for (cur = ctx->containers; cur != NULL; cur = cur->next) {
        unsigned long soft_mib = cur->soft_limit_bytes >> 20;
        unsigned long hard_mib = cur->hard_limit_bytes >> 20;
        printf("%s\t%d\t%s\t%lu\t%lu\t%ld\n",
               cur->id,
               (int)cur->host_pid,
               state_to_string(cur->state),
               soft_mib,
               hard_mib,
               (long)cur->started_at);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void dump_container_logs(const char *id)
{
    char path[PATH_MAX];
    FILE *fp;
    char buf[4096];
    size_t n;

    snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, id);

    fp = fopen(path, "r");
    if (fp == NULL) {
        perror("fopen logs");
        return;
    }

    while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) {
        fwrite(buf, 1, n, stdout);
    }

    fclose(fp);
}

static int stop_container(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *rec;
    pid_t pid = -1;

    pthread_mutex_lock(&ctx->metadata_lock);
    rec = find_container_record_locked(ctx->containers, id);
    if (rec != NULL) {
        pid = rec->host_pid;
        rec->state = CONTAINER_STOPPED;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pid < 0) {
        fprintf(stderr, "No such container: %s\n", id);
        return -1;
    }

    if (kill(pid, SIGTERM) < 0) {
        perror("kill");
        return -1;
    }

    return 0;
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        container_record_t *rec;
        char container_id[CONTAINER_ID_LEN];
        int should_unregister = 0;

        container_id[0] = '\0';

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_by_pid_locked(ctx->containers, pid);
        if (rec != NULL) {
            copy_string(container_id, sizeof(container_id), rec->id);
            if (WIFEXITED(status)) {
                rec->state = CONTAINER_EXITED;
                rec->exit_code = WEXITSTATUS(status);
                rec->exit_signal = 0;
            } else if (WIFSIGNALED(status)) {
                rec->state = CONTAINER_KILLED;
                rec->exit_code = -1;
                rec->exit_signal = WTERMSIG(status);
            }
            should_unregister = 1;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (should_unregister && ctx->monitor_fd >= 0 && container_id[0] != '\0') {
            (void)unregister_from_monitor(ctx->monitor_fd, container_id, pid);
        }
    }

    g_reap_children_requested = 0;
}

static int parse_control_request_line(const char *line, control_request_t *req)
{
    char *copy;
    char *save = NULL;
    char *tok;
    int have_kind = 0;

    if (line == NULL || req == NULL)
        return -1;

    memset(req, 0, sizeof(*req));
    copy = strdup(line);
    if (copy == NULL)
        return -1;

    for (tok = strtok_r(copy, " \t\r\n", &save);
         tok != NULL;
         tok = strtok_r(NULL, " \t\r\n", &save)) {
        char *eq = strchr(tok, '=');
        const char *key;
        const char *val;

        if (eq == NULL)
            continue;

        *eq = '\0';
        key = tok;
        val = eq + 1;

        if (strcmp(key, "kind") == 0) {
            req->kind = (command_kind_t)atoi(val);
            have_kind = 1;
        } else if (strcmp(key, "id") == 0) {
            copy_string(req->container_id, sizeof(req->container_id), val);
        } else if (strcmp(key, "rootfs") == 0) {
            copy_string(req->rootfs, sizeof(req->rootfs), val);
        } else if (strcmp(key, "command") == 0) {
            copy_string(req->command, sizeof(req->command), val);
        } else if (strcmp(key, "soft") == 0) {
            req->soft_limit_bytes = strtoul(val, NULL, 10);
        } else if (strcmp(key, "hard") == 0) {
            req->hard_limit_bytes = strtoul(val, NULL, 10);
        } else if (strcmp(key, "nice") == 0) {
            req->nice_value = atoi(val);
        }
    }

    free(copy);
    return have_kind ? 0 : -1;
}

static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    ensure_logs_dir();

    while (1) {
        int rc = bounded_buffer_pop(&ctx->log_buffer, &item);
        char path[PATH_MAX];
        FILE *fp;

        if (rc == 1)
            break;
        if (rc != 0)
            continue;

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        fp = fopen(path, "a");
        if (fp == NULL) {
            perror("fopen log");
            continue;
        }

        fwrite(item.data, 1, item.length, fp);
        fclose(fp);
    }

    return NULL;
}

static void *container_log_reader_thread(void *arg)
{
    log_pipe_ctx_t *ctx = (log_pipe_ctx_t *)arg;
    char buffer[LOG_CHUNK_SIZE];

    while (1) {
        ssize_t n = read(ctx->read_fd, buffer, sizeof(buffer));
        if (n == 0)
            break;
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                continue;
            perror("read container pipe");
            break;
        }

        log_item_t item;
        memset(&item, 0, sizeof(item));
        copy_string(item.container_id, sizeof(item.container_id), ctx->container_id);
        item.length = (size_t)n;
        if (item.length > sizeof(item.data))
            item.length = sizeof(item.data);
        memcpy(item.data, buffer, item.length);

        if (bounded_buffer_push(ctx->buffer, &item) != 0)
            break;
    }

    close(ctx->read_fd);

    if (ctx->stack != NULL)
        free(ctx->stack);
    if (ctx->cfg != NULL)
        free(ctx->cfg);
    free(ctx);
    return NULL;
}

static int spawn_container(supervisor_ctx_t *ctx, const control_request_t *req)
{
    int pipefd[2];
    void *stack;
    child_config_t *cfg;
    log_pipe_ctx_t *reader_ctx;
    pid_t pid;
    int flags;

    if (pipe(pipefd) < 0) {
        perror("pipe");
        return -1;
    }

    stack = malloc(STACK_SIZE);
    if (stack == NULL) {
        perror("malloc stack");
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    cfg = calloc(1, sizeof(*cfg));
    if (cfg == NULL) {
        perror("calloc cfg");
        free(stack);
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    copy_string(cfg->id, sizeof(cfg->id), req->container_id);
    copy_string(cfg->rootfs, sizeof(cfg->rootfs), req->rootfs);
    copy_string(cfg->command, sizeof(cfg->command), req->command);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = pipefd[1];

    flags = CLONE_NEWUTS | CLONE_NEWNS | CLONE_NEWPID | SIGCHLD;
    pid = clone(child_fn, (char *)stack + STACK_SIZE, flags, cfg);
    if (pid < 0) {
        perror("clone");
        free(cfg);
        free(stack);
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    close(pipefd[1]);

    pthread_mutex_lock(&ctx->metadata_lock);
    add_container_record_locked(ctx, req, pid);
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd,
                                  req->container_id,
                                  pid,
                                  req->soft_limit_bytes,
                                  req->hard_limit_bytes) < 0) {
            perror("register_with_monitor");
        }
    }

    reader_ctx = calloc(1, sizeof(*reader_ctx));
    if (reader_ctx == NULL) {
        perror("calloc reader_ctx");
        close(pipefd[0]);
        return 0;
    }

    reader_ctx->read_fd = pipefd[0];
    reader_ctx->buffer = &ctx->log_buffer;
    copy_string(reader_ctx->container_id, sizeof(reader_ctx->container_id), req->container_id);
    reader_ctx->stack = stack;
    reader_ctx->cfg = cfg;

    {
        pthread_t reader_thread;
        int rc = pthread_create(&reader_thread, NULL, container_log_reader_thread, reader_ctx);
        if (rc != 0) {
            errno = rc;
            perror("pthread_create reader");
            close(pipefd[0]);
            free(reader_ctx->stack);
            free(reader_ctx->cfg);
            free(reader_ctx);
            return -1;
        }
        pthread_detach(reader_thread);
    }

    printf("Started container %s (pid=%d)\n", req->container_id, (int)pid);
    return 0;
}

void handle_signal(int sig)
{
    if (sig == SIGINT || sig == SIGTERM) {
        g_shutdown_requested = 1;
    } else if (sig == SIGCHLD) {
        g_reap_children_requested = 1;
    }
}

int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    if (sethostname(cfg->id, strlen(cfg->id)) < 0) {
        perror("sethostname");
    }

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0) {
        perror("mount private");
    }

    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("chdir");
        return 1;
    }

    if (mkdir("/proc", 0555) < 0 && errno != EEXIST) {
        perror("mkdir /proc");
    }

    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount /proc");
    }

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0) {
        perror("dup2 stdout");
        return 1;
    }

    if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2 stderr");
        return 1;
    }

    close(cfg->log_write_fd);

    if (cfg->nice_value != 0)
        nice(cfg->nice_value);

    if (cfg->command[0] == '\0') {
        execl("/bin/sh", "sh", (char *)NULL);
    } else {
        execl("/bin/sh", "sh", "-lc", cfg->command, (char *)NULL);
    }

    perror("execl");
    return 1;
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
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
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
    int read_fd = -1;
    int keepalive_fd = -1;
    pthread_t logger_thread;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    ctx.should_stop = 0;
    ctx.containers = NULL;

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

    ensure_logs_dir();

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        perror("open /dev/container_monitor");
    }

    rc = pthread_create(&logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }
    ctx.logger_thread = logger_thread;

    if (unlink(CONTROL_FIFO_PATH) < 0 && errno != ENOENT) {
        perror("unlink control fifo");
    }

    if (mkfifo(CONTROL_FIFO_PATH, 0666) < 0 && errno != EEXIST) {
        perror("mkfifo");
        goto cleanup;
    }

    read_fd = open(CONTROL_FIFO_PATH, O_RDONLY | O_NONBLOCK);
    if (read_fd < 0) {
        perror("open fifo read");
        goto cleanup;
    }

    keepalive_fd = open(CONTROL_FIFO_PATH, O_WRONLY | O_NONBLOCK);
    if (keepalive_fd < 0) {
        perror("open fifo keepalive");
    }

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    signal(SIGCHLD, handle_signal);

    printf("Supervisor started with base rootfs: %s\n", rootfs);
    printf("Listening on FIFO: %s\n", CONTROL_FIFO_PATH);

    while (!g_shutdown_requested) {
        char buffer[512];
        ssize_t n;

        if (g_reap_children_requested)
            reap_children(&ctx);

        memset(buffer, 0, sizeof(buffer));
        n = read(read_fd, buffer, sizeof(buffer) - 1);
        if (n > 0) {
            char *save = NULL;
            char *line = strtok_r(buffer, "\n", &save);

            while (line != NULL) {
                control_request_t req;

                if (parse_control_request_line(line, &req) == 0) {
                    switch (req.kind) {
                    case CMD_START:
                    case CMD_RUN:
                        if (spawn_container(&ctx, &req) != 0) {
                            fprintf(stderr, "Failed to start container %s\n", req.container_id);
                        }
                        break;
                    case CMD_PS:
                        dump_containers(&ctx);
                        break;
                    case CMD_LOGS:
                        dump_container_logs(req.container_id);
                        break;
                    case CMD_STOP:
                        if (stop_container(&ctx, req.container_id) != 0) {
                            fprintf(stderr, "Failed to stop container %s\n", req.container_id);
                        }
                        break;
                    case CMD_SUPERVISOR:
                        break;
                    default:
                        fprintf(stderr, "Unknown command kind: %d\n", (int)req.kind);
                        break;
                    }
                } else {
                    printf("Received request: %s\n", line);
                }

                line = strtok_r(NULL, "\n", &save);
            }
        } else if (n < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                perror("read fifo");
                break;
            }
        }

        usleep(100000);
    }

    printf("\nShutting down supervisor...\n");

cleanup:
    g_shutdown_requested = 1;
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    if (keepalive_fd >= 0)
        close(keepalive_fd);
    if (read_fd >= 0)
        close(read_fd);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    unlink(CONTROL_FIFO_PATH);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
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
    char line[512];

    if (req == NULL) {
        fprintf(stderr, "Invalid request\n");
        return 1;
    }

    fd = open(CONTROL_FIFO_PATH, O_WRONLY | O_NONBLOCK);
    if (fd < 0) {
        perror("open control fifo");
        return 1;
    }

    snprintf(line, sizeof(line),
             "kind=%d id=%s rootfs=%s command=%s soft=%lu hard=%lu nice=%d\n",
             (int)req->kind,
             req->container_id,
             req->rootfs,
             req->command,
             req->soft_limit_bytes,
             req->hard_limit_bytes,
             req->nice_value);

    if (write(fd, line, strlen(line)) < 0) {
        perror("write control fifo");
        close(fd);
        return 1;
    }

    close(fd);
    return 0;
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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
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

    /*
     * TODO:
     * The supervisor should respond with container metadata.
     * Keep the rendering format simple enough for demos and debugging.
     */
    printf("Expected states include: %s, %s, %s, %s, %s\n",
           state_to_string(CONTAINER_STARTING),
           state_to_string(CONTAINER_RUNNING),
           state_to_string(CONTAINER_STOPPED),
           state_to_string(CONTAINER_KILLED),
           state_to_string(CONTAINER_EXITED));
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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

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