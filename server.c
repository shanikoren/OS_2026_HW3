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
    Worker **m_FreeWorkers;  /* circular buffer of Request pointers */
    int m_FWHead;
    int m_FWTail;
    int m_maxWorkers;
    int m_FWQCurrentSize;
} FW_Queue; //Free Worker Queue 


typedef struct {
    FW_Queue  *TPs_fwq;
    Req_Queue *TPs_rq;
    Worker    **m_AllWorkersArray;
} thread_pool;

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
/******************************* FW  queue struct ******************************/
void fw_queue_init(FW_Queue *fwq, int maxSize) {
    fwq->m_FreeWorkers = malloc(maxSize * sizeof(Worker *));
    if (fwq->m_FreeWorkers == NULL) unix_error("malloc failed");
    fwq->m_FWQCurrentSize = 0;
    fwq->m_FWHead         = 0;
    fwq->m_FWTail         = 0;
    fwq->m_maxWorkers     = maxSize;
}

void fw_queue_destroy(FW_Queue *fwq) {
    free(fwq->m_FreeWorkers);
}

void fwq_enqueue(FW_Queue *fwq, Worker *w) {
    fwq->m_FreeWorkers[fwq->m_FWTail] = w;
    fwq->m_FWTail = (fwq->m_FWTail + 1) % fwq->m_maxWorkers;
    fwq->m_FWQCurrentSize++;
}

Worker *fwq(FW_Queue *fwq) {
    Worker *w = fwq->m_FreeWorkers[fwq->m_FWHead];
    fwq->m_FWHead = (fwq->m_FWHead + 1) % fwq->m_maxWorkers;
    fwq->m_FWQCurrentSize--;
    return w;
}
/******************************* FW   queue  struct ******************************/
/******************************* ThreadPool  struct ******************************/

void thread_pool_init(thread_pool *tp, int MaxReqs, int MaxWorkers) {
    tp->TPs_rq = malloc(sizeof(Req_Queue));
    if (tp->TPs_rq == NULL) unix_error("malloc failed");
    Req_queue_init(tp->TPs_rq, MaxReqs);

    tp->TPs_fwq = malloc(sizeof(FW_Queue));
    if (tp->TPs_fwq == NULL) unix_error("malloc failed");
    fw_queue_init(tp->TPs_fwq, MaxWorkers);
}

void thread_pool_destroy(thread_pool *tp) {
    Req_queue_destroy(tp->TPs_rq);
    free(tp->TPs_rq);
    fw_queue_destroy(tp->TPs_fwq);
    free(tp->TPs_fwq);
}


// TODO: HW3 — Task 1: Initialize the thread pool and request queue.
// This server currently handles all requests in the main thread.

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
    thread_pool_init(&tp, queueSize, numThreads);

    listenfd = Open_listenfd(tcpPort);
    while (1) {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t*) &clientlen);

        // TODO: HW3 — Record the request arrival time here.

        // DEMO PURPOSE ONLY:
        // This is a dummy request handler that immediately processes the
        // request in the master thread without concurrency. Replace this with
        // logic that enqueues the connection so a worker thread handles it.
        
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
