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
#include <sys/resource.h>
#include <sys/poll.h>
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
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum
{
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum
{
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record
{
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

typedef struct
{
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct
{
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct
{
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct
{
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct
{
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
    int log_read_fd;
} child_config_t;

typedef struct
{
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

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
    if (errno != 0 || end == value || *end != '\0')
    {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20))
    {
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

    for (i = start_index; i < argc; i += 2)
    {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc)
        {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0)
        {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0)
        {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0)
        {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19)
            {
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

    if (req->soft_limit_bytes > req->hard_limit_bytes)
    {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state)
    {
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

/* Metadata table helpers (must call with ctx->metadata_lock held) */
static container_record_t *metadata_find_by_id(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *rec;
    for (rec = ctx->containers; rec; rec = rec->next)
    {
        if (strcmp(rec->id, id) == 0)
            return rec;
    }
    return NULL;
}

static container_record_t *metadata_find_by_pid(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *rec;
    for (rec = ctx->containers; rec; rec = rec->next)
    {
        if (rec->host_pid == pid)
            return rec;
    }
    return NULL;
}

static int metadata_insert(supervisor_ctx_t *ctx, const char *id, pid_t pid,
                           unsigned long soft_limit, unsigned long hard_limit,
                           const char *log_path)
{
    container_record_t *rec = malloc(sizeof(*rec));
    if (!rec)
        return -1;

    strncpy(rec->id, id, sizeof(rec->id) - 1);
    rec->id[sizeof(rec->id) - 1] = '\0';
    rec->host_pid = pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = soft_limit;
    rec->hard_limit_bytes = hard_limit;
    rec->exit_code = 0;
    rec->exit_signal = 0;
    strncpy(rec->log_path, log_path, sizeof(rec->log_path) - 1);
    rec->log_path[sizeof(rec->log_path) - 1] = '\0';

    /* Insert at head */
    rec->next = ctx->containers;
    ctx->containers = rec;
    return 0;
}

static void metadata_remove(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t **pp, *rec;
    for (pp = &ctx->containers; *pp; pp = &(*pp)->next)
    {
        rec = *pp;
        if (rec->host_pid == pid)
        {
            *pp = rec->next;
            free(rec);
            return;
        }
    }
}

static void metadata_print_all(supervisor_ctx_t *ctx)
{
    container_record_t *rec;
    printf("Container Metadata:\n");
    printf("%-20s %-10s %-15s %-10s\n", "ID", "PID", "State", "Exit");
    for (rec = ctx->containers; rec; rec = rec->next)
    {
        printf("%-20s %-10d %-15s %-10d\n", rec->id, rec->host_pid,
               state_to_string(rec->state), rec->exit_code);
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
    if (rc != 0)
    {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0)
    {
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

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

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
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* Producer thread context */
typedef struct {
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
    supervisor_ctx_t *ctx;
} producer_ctx_t;

/* Producer thread function */
static void *producer_thread(void *arg)
{
    producer_ctx_t *pctx = (producer_ctx_t *)arg;
    char buf[LOG_CHUNK_SIZE];
    ssize_t n;

    while ((n = read(pctx->read_fd, buf, sizeof(buf))) > 0) {
        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, pctx->container_id, sizeof(item.container_id) - 1);
        item.length = n;
        memcpy(item.data, buf, n);
        
        if (bounded_buffer_push(&pctx->ctx->log_buffer, &item) < 0) {
            break; /* shutting down */
        }
        fprintf(stderr, "[LOGGER] Producer: captured %zd bytes from '%s'\n", n, pctx->container_id);
    }
    
    close(pctx->read_fd);
    free(pctx);
    return NULL;
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
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char log_path[PATH_MAX];
        snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, item.container_id);

        int fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) {
            write(fd, item.data, item.length);
            close(fd);
            fprintf(stderr, "[LOGGER] Consumer: wrote %zu bytes for '%s'\n", item.length, item.container_id);
        }
    }
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
    child_config_t *cfg = (child_config_t *)arg;

    if (cfg->log_read_fd >= 0) {
        close(cfg->log_read_fd);
    }
    if (cfg->log_write_fd >= 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }

    /* Change to container rootfs directory */
    if (chdir(cfg->rootfs) < 0)
    {
        perror("chdir");
        return 1;
    }

    /* Set root to container rootfs */
    if (chroot(cfg->rootfs) < 0)
    {
        perror("chroot");
        return 1;
    }

    /* Mount /proc inside the container so ps, etc. work */
    if (mount("proc", "/proc", "proc", 0, NULL) < 0)
    {
        perror("mount /proc");
        return 1;
    }

    /* Set nice value if requested */
    if (cfg->nice_value != 0)
    {
        if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0)
        {
            perror("setpriority");
            return 1;
        }
    }

    /* Execute the command */
    execl("/bin/sh", "sh", "-c", cfg->command, NULL);
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

static int spawn_container(supervisor_ctx_t *ctx,
                           const char *container_id,
                           const char *container_rootfs,
                           const char *command,
                           unsigned long soft_limit,
                           unsigned long hard_limit,
                           int nice_value);

static int init_control_socket(void)
{
    int fd;
    struct sockaddr_un addr;

    unlink(CONTROL_PATH);

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    if (listen(fd, 4) < 0) {
        close(fd);
        return -1;
    }

    return fd;
}

static int process_control_request(supervisor_ctx_t *ctx,
                                   const control_request_t *req,
                                   control_response_t *resp)
{
    int rc = 0;
    char buffer[CONTROL_MESSAGE_LEN];

    memset(resp, 0, sizeof(*resp));

    switch (req->kind) {
    case CMD_START:
        pthread_mutex_lock(&ctx->metadata_lock);
        if (metadata_find_by_id(ctx, req->container_id) != NULL) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = 1;
            snprintf(resp->message, sizeof(resp->message), "Container '%s' already exists",
                     req->container_id);
            return resp->status;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        rc = spawn_container(ctx, req->container_id, req->rootfs, req->command,
                             req->soft_limit_bytes, req->hard_limit_bytes,
                             req->nice_value);
        if (rc < 0) {
            resp->status = 1;
            snprintf(resp->message, sizeof(resp->message), "Failed to start container '%s'",
                     req->container_id);
            return resp->status;
        }
        snprintf(resp->message, sizeof(resp->message), "Started container '%s'", req->container_id);
        return 0;

    case CMD_PS:
        pthread_mutex_lock(&ctx->metadata_lock);
        snprintf(buffer, sizeof(buffer), "%-10s %-7s %-10s %-12s\n",
                 "ID", "PID", "State", "Exit");
        size_t len = strlen(buffer);
        for (container_record_t *rec = ctx->containers; rec; rec = rec->next) {
            int written = snprintf(buffer + len, sizeof(buffer) - len,
                                   "%-10s %-7d %-10s %-12d\n",
                                   rec->id, rec->host_pid,
                                   state_to_string(rec->state), rec->exit_code);
            if (written < 0 || (size_t)written >= sizeof(buffer) - len)
                break;
            len += written;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 0;
        strncpy(resp->message, buffer, sizeof(resp->message) - 1);
        return 0;

    case CMD_STOP:
        pthread_mutex_lock(&ctx->metadata_lock);
        {
            container_record_t *rec = metadata_find_by_id(ctx, req->container_id);
            if (!rec) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                resp->status = 1;
                snprintf(resp->message, sizeof(resp->message), "No such container '%s'",
                         req->container_id);
                return resp->status;
            }
            if (rec->state == CONTAINER_EXITED) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                resp->status = 1;
                snprintf(resp->message, sizeof(resp->message), "Container '%s' already exited",
                         req->container_id);
                return resp->status;
            }
        kill(rec->host_pid, SIGKILL);
            rec->state = CONTAINER_STOPPED;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(resp->message, sizeof(resp->message), "Stop requested for '%s'", req->container_id);
        return 0;

    case CMD_LOGS:
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "logs command not implemented yet");
        return resp->status;

    case CMD_RUN:
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "run command not implemented yet");
        return resp->status;

    default:
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "Unknown command kind %d", req->kind);
        return resp->status;
    }
}

static void handle_control_connection(supervisor_ctx_t *ctx, int conn_fd)
{
    control_request_t req;
    control_response_t resp;
    ssize_t n;

    n = recv(conn_fd, &req, sizeof(req), MSG_WAITALL);
    if (n != sizeof(req)) {
        close(conn_fd);
        return;
    }

    process_control_request(ctx, &req, &resp);
    send(conn_fd, &resp, sizeof(resp), 0);
    close(conn_fd);
}

/* SIGCHLD handler - just set a flag; real reaping happens in event loop */
static volatile sig_atomic_t sigchld_received = 0;
static volatile sig_atomic_t supervisor_interrupted = 0;

static void sigchld_handler(int sig)
{
    (void)sig;
    sigchld_received = 1;
}

static void sigint_handler(int sig)
{
    (void)sig;
    supervisor_interrupted = 1;
    fprintf(stderr, "\nReceived SIGINT, shutting down...\n");
}

/* Helper: spawn a new container via clone() */
static int spawn_container(supervisor_ctx_t *ctx,
                           const char *container_id,
                           const char *container_rootfs,
                           const char *command,
                           unsigned long soft_limit,
                           unsigned long hard_limit,
                           int nice_value)
{
    char log_path[PATH_MAX];
    child_config_t child_cfg;
    char *stack_ptr;
    pid_t child_pid;
    int rc;

    /* Create log directory if needed */
    mkdir(LOG_DIR, 0755);

    /* Build log path */
    snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, container_id);

    /* Prepare child config */
    memset(&child_cfg, 0, sizeof(child_cfg));
    strncpy(child_cfg.id, container_id, sizeof(child_cfg.id) - 1);
    strncpy(child_cfg.rootfs, container_rootfs, sizeof(child_cfg.rootfs) - 1);
    strncpy(child_cfg.command, command, sizeof(child_cfg.command) - 1);
    child_cfg.nice_value = nice_value;

    /* Setup logging pipes */
    int pipefd[2];
    if (pipe(pipefd) < 0) {
        perror("pipe");
        return -1;
    }
    child_cfg.log_read_fd = pipefd[0];
    child_cfg.log_write_fd = pipefd[1];

    /* Allocate stack for cloned process */
    stack_ptr = malloc(STACK_SIZE);
    if (!stack_ptr)
        return -1;

    /* Clone with PID, UTS, mount namespaces */
    child_pid = clone(child_fn,
                      stack_ptr + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS |
                          SIGCHLD, /* deliver SIGCHLD to parent on child exit */
                      &child_cfg);

    if (child_pid < 0)
    {
        perror("clone");
        free(stack_ptr);
        return -1;
    }

    /* Parent closes write end of the pipe */
    close(pipefd[1]);

    /* Start producer thread for this container */
    producer_ctx_t *pctx = malloc(sizeof(*pctx));
    pctx->read_fd = pipefd[0];
    pctx->ctx = ctx;
    strncpy(pctx->container_id, container_id, sizeof(pctx->container_id) - 1);
    pthread_t prod_tid;
    pthread_create(&prod_tid, NULL, producer_thread, pctx);
    pthread_detach(prod_tid);

    fprintf(stderr, "[INFO] Spawned container '%s' with PID %d\n", container_id, child_pid);

    /* Register with kernel monitor if available */
    if (ctx->monitor_fd >= 0)
    {
        rc = register_with_monitor(ctx->monitor_fd, container_id, child_pid, soft_limit, hard_limit);
        if (rc < 0)
            fprintf(stderr, "[WARN] Failed to register container with monitor\n");
    }

    /* Record in metadata */
    pthread_mutex_lock(&ctx->metadata_lock);
    rc = metadata_insert(ctx, container_id, child_pid, soft_limit, hard_limit, log_path);
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (rc < 0)
    {
        fprintf(stderr, "[ERROR] Failed to insert metadata\n");
        free(stack_ptr);
        return -1;
    }

    free(stack_ptr); /* Safe to free after clone; child has own stack */
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
    struct sigaction sa;
    int rc, keep_going = 1;
    pid_t child_pid;
    int status;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0)
    {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0)
    {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* Create log directory */
    mkdir(LOG_DIR, 0755);

    /* Start logging thread */
    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        perror("pthread_create logger");
        return 1;
    }

    /* Try to open kernel monitor device (optional for Phase 2) */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
    {
        fprintf(stderr, "[WARN] Could not open /dev/container_monitor (expected if module not loaded)\n");
        ctx.monitor_fd = -1;
    }

    /* Install signal handlers */
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGCHLD, &sa, NULL);

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigint_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGINT, &sa, NULL);

    fprintf(stderr, "[INFO] Supervisor started with rootfs: %s\n", rootfs);
    fprintf(stderr, "[INFO] Main event loop starting. Waiting for IPC requests...\n");

    ctx.server_fd = init_control_socket();
    if (ctx.server_fd < 0) {
        fprintf(stderr, "[ERROR] Failed to initialize control socket\n");
        return 1;
    }

    /* Main event loop */
    while (keep_going)
    {
        /* Check for SIGCHLD and reap any dead children */
        if (sigchld_received)
        {
            sigchld_received = 0;

            while ((child_pid = waitpid(-1, &status, WNOHANG)) > 0)
            {
                container_record_t *rec;

                fprintf(stderr, "[INFO] Child PID %d exited with status %d\n", child_pid, status);

                pthread_mutex_lock(&ctx.metadata_lock);
                rec = metadata_find_by_pid(&ctx, child_pid);
                if (rec)
                {
                    if (WIFEXITED(status))
                    {
                        rec->exit_code = WEXITSTATUS(status);
                        rec->exit_signal = 0;
                    }
                    else if (WIFSIGNALED(status))
                    {
                        rec->exit_signal = WTERMSIG(status);
                        rec->exit_code = 128 + rec->exit_signal;
                    }
                    
                    /* Differentiate graceful exits, manual stops, and kernel hard-limit kills */
                    if (rec->state != CONTAINER_STOPPED) {
                        if (WIFSIGNALED(status) && rec->exit_signal == SIGKILL) {
                            rec->state = CONTAINER_KILLED;
                        } else {
                            rec->state = CONTAINER_EXITED;
                        }
                    }
                    fprintf(stderr, "[INFO] Container '%s' exited with code %d, signal %d\n",
                            rec->id, rec->exit_code, rec->exit_signal);
                }
                pthread_mutex_unlock(&ctx.metadata_lock);

                /* Unregister from monitor if available */
                if (ctx.monitor_fd >= 0 && rec)
                {
                    unregister_from_monitor(ctx.monitor_fd, rec->id, child_pid);
                }
            }
        }

        if (supervisor_interrupted) {
            keep_going = 0;
            break;
        }

        struct pollfd pfd;
        pfd.fd = ctx.server_fd;
        pfd.events = POLLIN;

        /* Wait up to 200ms for incoming connections. 
         * The timeout ensures we periodically loop back to check signals. */
        int ret = poll(&pfd, 1, 200);
        if (ret < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }

        if (ret > 0 && (pfd.revents & POLLIN)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd >= 0) {
                handle_control_connection(&ctx, client_fd);
            }
        }
    }

    fprintf(stderr, "[INFO] Supervisor shutting down...\n");

    /* Gracefully terminate all containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *rec;
    for (rec = ctx.containers; rec; rec = rec->next)
    {
        if (rec->state != CONTAINER_EXITED)
        {
            fprintf(stderr, "[INFO] Terminating container '%s' (PID %d)\n", rec->id, rec->host_pid);
            kill(rec->host_pid, SIGKILL);
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Give containers time to exit gracefully */
    sleep(1);

    /* Reap any remaining children */
    while ((child_pid = waitpid(-1, &status, WNOHANG)) > 0)
    {
        fprintf(stderr, "[INFO] Reaped child PID %d\n", child_pid);
    }

    /* Clean up */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    if (ctx.server_fd >= 0)
        close(ctx.server_fd);

    /* Free container metadata records */
    container_record_t *curr = ctx.containers;
    while (curr) {
        container_record_t *next = curr->next;
        free(curr);
        curr = next;
    }

    pthread_mutex_destroy(&ctx.metadata_lock);

    fprintf(stderr, "[INFO] Supervisor exited cleanly\n");
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
    ssize_t n;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Error connecting to supervisor (is it running?)\n");
        close(fd);
        return 1;
    }

    if (send(fd, req, sizeof(*req), 0) != sizeof(*req)) {
        perror("send");
        close(fd);
        return 1;
    }

    n = recv(fd, &resp, sizeof(resp), MSG_WAITALL);
    if (n != sizeof(resp)) {
        fprintf(stderr, "Error receiving response from supervisor\n");
        close(fd);
        return 1;
    }

    if (resp.message[0] != '\0') {
        printf("%s\n", resp.message);
    }

    close(fd);
    return resp.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5)
    {
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

    if (argc < 5)
    {
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

    if (argc < 3)
    {
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

    if (argc < 3)
    {
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
    if (argc < 2)
    {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0)
    {
        if (argc < 3)
        {
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
