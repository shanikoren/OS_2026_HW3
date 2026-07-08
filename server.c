#include "segel.h"
#include "request.h"
#include "log.h"

//[] queue free workers
//   () 
//[] array all workers
//[] pull out the Req queue to nifrad
//  () pull out init & destroy
//  () 
//[] le'adken getargs
//[] lesader main


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
void getargs(int *tcpPort, int *udpPort, int *numThreads, int *queueSize, double *debugSleepTime, int argc, char *argv[])
{
    if (argc < 6) {
        fprintf(stderr, "Usage: %s <tcp_portnum> <udp_portnum> <threads> <queue_size> <debug_sleep_time>\n", argv[0]);
        exit(1);
    }
    *tcpPort        = atoi(argv[1]);
    *udpPort        = atoi(argv[2]);
    *numThreads     = atoi(argv[3]);
    *queueSize      = atoi(argv[4]);
    *debugSleepTime = atof(argv[5]);
}


typedef struct {
    Request **m_items;  /* circular buffer of Request pointers */
    int m_reqHead;
    int m_reqTail;
    int m_maxRequests;
    pthread_cond_t  RQisFull;
    pthread_cond_t  RQisEmpty;
    pthread_mutex_t RQMutex;
    int m_RQCurrentSize;
} Req_Queue;

typedef struct {
    Req_Queue       *TPs_rq;
    threads_stats   *m_StatsArray;
    pthread_t       *m_ThreadIds;
    int              m_numWorkers;
    server_log       TPs_log;
} thread_pool;

typedef struct {
    thread_pool     *tp;
    int              worker_idx;
} worker_arg;

static void *worker_routine(void *arg);

/*******************************Req queue struct******************************/
void Req_queue_init(Req_Queue *rq, int maxSize) {
    rq->m_items = malloc(maxSize * sizeof(Request *));
    if (rq->m_items == NULL) unix_error("malloc failed");
    rq->m_RQCurrentSize = 0;
    rq->m_reqHead       = 0;
    rq->m_reqTail       = 0;
    rq->m_maxRequests   = maxSize;

    pthread_cond_init(&rq->RQisFull, NULL);
    pthread_cond_init(&rq->RQisEmpty, NULL);
    int rc = pthread_mutex_init(&rq->RQMutex, NULL);
    if (rc != 0) posix_error(rc, "pthread_mutex_init failed");
}

void Req_queue_destroy(Req_Queue *rq) {
    free(rq->m_items);
    pthread_cond_destroy(&rq->RQisFull);
    pthread_cond_destroy(&rq->RQisEmpty);
    pthread_mutex_destroy(&rq->RQMutex);
}

void rq_enqueue(Req_Queue *rq, Request *x) {
    pthread_mutex_lock(&(rq->RQMutex));
    while(rq->m_RQCurrentSize == rq->m_maxRequests) {
        pthread_cond_wait(&(rq->RQisFull), &(rq->RQMutex));
    }
    rq->m_items[rq->m_reqTail] = x;
    rq->m_reqTail = (rq->m_reqTail + 1) % rq->m_maxRequests;
    rq->m_RQCurrentSize++;
    pthread_cond_signal(&(rq->RQisEmpty));
    pthread_mutex_unlock(&(rq->RQMutex));
}

Request *rq_queue_dequeue(Req_Queue *rq) {
    pthread_mutex_lock(&(rq->RQMutex));
    while(rq->m_RQCurrentSize == 0) {
        pthread_cond_wait(&(rq->RQisEmpty), &(rq->RQMutex));
    }
    Request *x = rq->m_items[rq->m_reqHead];
    rq->m_reqHead = (rq->m_reqHead + 1) % rq->m_maxRequests;
    rq->m_RQCurrentSize--;
    pthread_cond_signal(&(rq->RQisFull));
    pthread_mutex_unlock(&(rq->RQMutex));
    return x;
}
/******************************* Req queue struct ******************************/
/******************************* ThreadPool  struct ******************************/

void thread_pool_init(thread_pool *tp, int MaxReqs, int MaxWorkers, server_log log) {
    tp->TPs_rq = malloc(sizeof(Req_Queue));
    if (tp->TPs_rq == NULL) unix_error("malloc failed");
    Req_queue_init(tp->TPs_rq, MaxReqs);

    tp->TPs_log         = log;
    tp->m_numWorkers    = MaxWorkers;

    tp->m_StatsArray = malloc(MaxWorkers * sizeof(threads_stats));
    if (tp->m_StatsArray == NULL) unix_error("malloc failed");
    tp->m_ThreadIds = malloc(MaxWorkers * sizeof(pthread_t));
    if (tp->m_ThreadIds == NULL) unix_error("malloc failed");

    for (int i = 0; i < MaxWorkers; i++) {
        threads_stats stats = malloc(sizeof(struct Threads_stats));
        if (stats == NULL) unix_error("malloc failed");
        stats->id        = i + 1;
        stats->stat_req  = 0;
        stats->dynm_req  = 0;
        stats->post_req  = 0;
        stats->total_req = 0;
        tp->m_StatsArray[i] = stats;
        
        worker_arg *wa = malloc(sizeof(worker_arg));
        if (wa == NULL) unix_error("malloc failed");
        wa->tp = tp;
        wa->worker_idx = i;

        int rc = pthread_create(&tp->m_ThreadIds[i], NULL, worker_routine, wa);
        if (rc != 0) posix_error(rc, "pthread_create failed");
    }
}

// Each worker thread runs this loop for the lifetime of the server:
// pull the next connection off the shared queue and handle it.
static void *worker_routine(void *arg) {
    worker_arg *wa = (worker_arg *)arg;
    thread_pool *tp = wa->tp;
    threads_stats stats = tp->m_StatsArray[wa->worker_idx];
    free(wa);

    while (1) {
        Request *req = rq_queue_dequeue(tp->TPs_rq);

        gettimeofday(&req->m_timestats->task_dispatch, NULL);
        requestHandle(req->m_fd, *req->m_timestats, stats, tp->TPs_log);

        Close(req->m_fd);
        free(req->m_timestats);
        free(req);
    }
    return NULL;
}

void thread_pool_destroy(thread_pool *tp) {
    Req_queue_destroy(tp->TPs_rq);
    free(tp->TPs_rq);
    for (int i = 0; i < tp->m_numWorkers; i++) {
        free(tp->m_StatsArray[i]);
    }
    free(tp->m_StatsArray);
    free(tp->m_ThreadIds);
}


// TODO: HW3 — Task 4: Add the UDP channel (see the UDP_* wrappers in segel.c).

int main(int argc, char *argv[])
{
    int listenfd, connfd, tcpPort, udpPort, clientlen, numThreads, queueSize;
    double debugSleepTime;
    struct sockaddr_in clientaddr;

    getargs(&tcpPort, &udpPort, &numThreads, &queueSize, &debugSleepTime, argc, argv);

    // Create the global server log
    server_log log = create_log(debugSleepTime);

    thread_pool tp;
    thread_pool_init(&tp, queueSize, numThreads, log);

    listenfd = Open_listenfd(tcpPort);
    while (1) {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t*) &clientlen);

        Request *req = malloc(sizeof(Request));
        if (req == NULL) unix_error("malloc failed");
        req->m_fd = connfd;
        req->m_timestats = malloc(sizeof(time_stats));
        if (req->m_timestats == NULL) unix_error("malloc failed");
        gettimeofday(&req->m_timestats->task_arrival, NULL);

        // Hand the connection off to the queue; a worker thread will pick it up.
        rq_enqueue(tp.TPs_rq, req);
    }

    // Clean up the server log before exiting
    destroy_log(log);
    thread_pool_destroy(&tp);
}
