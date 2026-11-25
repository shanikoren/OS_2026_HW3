#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "log.h"
#include "request.h"
#include "segel.h"

static void take_time(struct timeval* t) {
    if (gettimeofday(t, NULL) < 0)
        unix_error("gettimeofday failed");
}

static void readers_writers_init(server_log log);

static void reader_lock(server_log log);

static void reader_unlock(server_log log);

static void writer_lock(server_log log);

static void writer_unlock(server_log log);

// Opaque struct definition
struct Server_Log {
    // TODO: Implement internal log storage (e.g., dynamic buffer, linked list, etc.)
    char* buffer;
    int size;
    int capacity;
    double debug_sleep_sec;
    int readers_inside, writers_inside, writers_waiting;
    pthread_cond_t read_allowed;
    pthread_cond_t write_allowed;
    pthread_mutex_t global_lock;
};

server_log create_log(double debug_sec) {
    server_log log = (server_log)malloc(sizeof(struct Server_Log));
    if (log == NULL) unix_error("malloc failed");
    log->size = 0;
    log->capacity = 1024; // buffer starting capacity
    log->buffer = (char*)malloc(log->capacity);
    if (log->buffer == NULL) unix_error("malloc failed");
    log->buffer[0] = '\0';
    log->debug_sleep_sec = debug_sec;
    readers_writers_init(log);
    return log;
}


void destroy_log(server_log log) {
    if (!log) return;
    free(log->buffer);
    pthread_cond_destroy(&log->read_allowed);
    pthread_cond_destroy(&log->write_allowed);
    pthread_mutex_destroy(&log->global_lock);
    free(log);
}

// Returns dummy log content as string (stub)
int get_log(server_log log, char** dst, struct Time_stats* tm_stats) {
    // TODO: Return the full contents of the log as a dynamically allocated string
    // This function should handle concurrent access
    if (tm_stats) take_time(&tm_stats->log_enter);
    reader_lock(log);
    if (log->debug_sleep_sec > 0) {
        usleep((unsigned int)(log->debug_sleep_sec * 1000000));
    }
    if (tm_stats) take_time(&tm_stats->log_exit);
    int n = log->size;
    *dst = malloc(n + 1);
    if (!*dst)
        unix_error("malloc failed");
    memcpy(*dst, log->buffer, n);
    (*dst)[n] = '\0';
    reader_unlock(log);
    return n;
}


// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, struct Threads_stats* t_stats, struct Time_stats* tm_stats) {
    // TODO: Append the provided data to the log
    // This function should handle concurrent access
    if (tm_stats) take_time(&tm_stats->log_enter);
    writer_lock(log);
    if (log->debug_sleep_sec > 0) {
        usleep((unsigned int)(log->debug_sleep_sec * 1000000));
    }
    if (tm_stats) take_time(&tm_stats->log_exit);
    char record[MAXBUF];
    record[0] = '\0';
    int rec_len = append_stats(record, t_stats, *tm_stats);
    int add_hash = (log->size > 0) ? 1 : 0;
    if (log->size + rec_len + add_hash + 1 > log->capacity) {
        log->capacity = (log->size + rec_len + add_hash + 1) * 2;   //buffer grows dynamically
        log->buffer = (char*)realloc(log->buffer, log->capacity);
        if (log->buffer == NULL) unix_error("realloc failed");
    }
    if (add_hash) log->buffer[log->size++] = '#';
    memcpy(log->buffer + log->size, record, rec_len);
    log->size += rec_len;
    log->buffer[log->size] = '\0';
    writer_unlock(log);
}

void readers_writers_init(server_log log) {
    log->readers_inside = 0;
    log->writers_inside = 0;
    log->writers_waiting = 0;
    pthread_cond_init(&log->read_allowed, NULL);
    pthread_cond_init(&log->write_allowed, NULL);
    pthread_mutex_init(&log->global_lock, NULL);
}

void reader_lock(server_log log) {
    pthread_mutex_lock(&log->global_lock);
    while (log->writers_inside > 0 || log->writers_waiting > 0)
        pthread_cond_wait(&log->read_allowed, &log->global_lock);
    log->readers_inside++;
    pthread_mutex_unlock(&log->global_lock);
}

void reader_unlock(server_log log) {
    pthread_mutex_lock(&log->global_lock);
    log->readers_inside--;
    if (log->readers_inside == 0)
        pthread_cond_signal(&log->write_allowed);
    pthread_mutex_unlock(&log->global_lock);
}

void writer_lock(server_log log) {
    pthread_mutex_lock(&log->global_lock);
    log->writers_waiting++;
    while (log->writers_inside + log->readers_inside > 0)
        pthread_cond_wait(&log->write_allowed, &log->global_lock);
    log->writers_waiting--;
    log->writers_inside++;
    pthread_mutex_unlock(&log->global_lock);
}

void writer_unlock(server_log log) {
    pthread_mutex_lock(&log->global_lock);
    log->writers_inside--;
    if (log->writers_inside == 0) {
        pthread_cond_broadcast(&log->read_allowed);
        pthread_cond_signal(&log->write_allowed);
    }
    pthread_mutex_unlock(&log->global_lock);
}
