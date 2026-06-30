#include "segel.h"
#include "request.h"
#include "log.h"

//
// server.c: A very, very simple web server
//
// To run:
//  ./server <portnum (above 2000)>
//
// Repeatedly handles HTTP requests sent to this port number.
// Most of the work is done within routines written in request.c
//

// Parses command-line arguments
void getargs(int *port, int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        exit(1);
    }
    *port = atoi(argv[1]);
}


/*******************************queue struct******************************/
typedef struct {
    Request **items;  /* circular buffer of Request pointers */
    int head;
    int tail;
    int maxSize;
} TP_Queue;

void tp_queue_init(TP_Queue *q, int maxSize) {
    q->items = malloc(maxSize * sizeof(Request *));
    //TODO: handle malloc failure
    q->head    = 0;
    q->tail    = 0;
    q->maxSize = maxSize;
}

void tp_queue_destroy(TP_Queue *q) {
    free(q->items);
}

void tp_queue_enqueue(TP_Queue *q, Request *x) {
    q->items[q->tail] = x;
    q->tail = (q->tail + 1) % q->maxSize;
}

Request *tp_queue_dequeue(TP_Queue *q) {
    Request *x = q->items[q->head];
    q->head = (q->head + 1) % q->maxSize;
    return x;
}
/*******************************queue struct******************************/

typedef struct {
    pthread_cond_t  isFull;
    pthread_mutex_t queueMutex;
    int queueMaxSize;
    int queueCurrentSize;
    TP_Queue queue;
} thread_pool;

void thread_pool_init(thread_pool *tp, int size) {
    tp->queueMaxSize     = size;
    tp->queueCurrentSize = 0;
    pthread_cond_init(&tp->isFull, NULL);
    if (pthread_mutex_init(&tp->queueMutex, NULL) != 0) {
        //TODO: print the error that segel asked.
    }
    tp_queue_init(&tp->queue, size);
}

void thread_pool_destroy(thread_pool *tp) {
    pthread_cond_destroy(&tp->isFull);
    pthread_mutex_destroy(&tp->queueMutex);
    tp_queue_destroy(&tp->queue);
}
// TODO: HW3 — Task 1: Initialize the thread pool and request queue.
// This server currently handles all requests in the main thread.

// TODO: HW3 — Task 4: Add the UDP channel (see the UDP_* wrappers in segel.c).

// TODO: HW3 — Extend getargs() to parse the full argument list.

int main(int argc, char *argv[])
{
    // Create the global server log
    server_log log = create_log();

    int listenfd, connfd, port, clientlen;
    struct sockaddr_in clientaddr;

    getargs(&port, argc, argv);

    listenfd = Open_listenfd(port);
    while (1) {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t*) &clientlen);

        // TODO: HW3 — Record the request arrival time here.

        // DEMO PURPOSE ONLY:
        // This is a dummy request handler that immediately processes the
        // request in the master thread without concurrency. Replace this with
        // logic that enqueues the connection so a worker thread handles it.

        threads_stats t = malloc(sizeof(struct Threads_stats));
        t->id = 0;             // Thread ID (placeholder)
        t->stat_req = 0;       // Static request count
        t->dynm_req = 0;       // Dynamic request count
        t->post_req = 0;       // POST request count
        t->total_req = 0;      // Total request count

        time_stats dum;

        // gettimeofday(&arrival, NULL);

        // Call the request handler (immediate in master thread — DEMO ONLY)
        requestHandle(connfd, dum, t, log);

        free(t); // Cleanup
        Close(connfd); // Close the connection
    }

    // Clean up the server log before exiting
    destroy_log(log);

    // TODO: HW3 — Add cleanup code for the thread pool and queue.
}
