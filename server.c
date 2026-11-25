#include "segel.h"
#include "request.h"
#include "log.h"

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

    if (*tcpPort <= 1024 || *tcpPort > 65535) {
        fprintf(stderr, "Error: tcp_port must be in (1024, 65535], got %d\n", *tcpPort);
        exit(1);
    }
    if (*udpPort <= 1024 || *udpPort > 65535) {
        fprintf(stderr, "Error: udp_port must be in (1024, 65535], got %d\n", *udpPort);
        exit(1);
    }
    if (*udpPort == *tcpPort) {
        fprintf(stderr, "Error: udp_port must differ from tcp_port (%d)\n", *tcpPort);
        exit(1);
    }
    if (*numThreads <= 0) {
        fprintf(stderr, "Error: threads must be a positive integer, got %d\n", *numThreads);
        exit(1);
    }
    if (*queueSize <= 0) {
        fprintf(stderr, "Error: queue_size must be a positive integer, got %d\n", *queueSize);
        exit(1);
    }
}

/******************************* Structs ******************************/

// A single pending UDP ping: the reply address, as a FIFO list node
typedef struct Ping {
    struct sockaddr_in addr;
    struct Ping *next;
} ping_t;

typedef struct {
    Request **m_items;  /* circular buffer of Request pointers */
    int m_reqHead;
    int m_reqTail;
    int m_maxRequests;
    int m_RQCurrentSize;

    // UDP mailboxes per worker: dynamic FIFO linked lists (no cap)
    ping_t **udp_mail_head;
    ping_t **udp_mail_tail;

    pthread_cond_t  RQisFull;
    pthread_cond_t  RQisEmpty;
    pthread_mutex_t RQMutex;
} Req_Queue;

typedef struct {
    Req_Queue       *TPs_rq;
    threads_stats   *m_StatsArray;
    pthread_t       *m_ThreadIds;
    int              m_numWorkers;
    server_log       TPs_log;
    int              udpfd;
} thread_pool;

typedef struct {
    thread_pool     *tp;
    int              worker_idx;
} worker_arg;


/******************************* Req queue functions ******************************/

void Req_queue_init(Req_Queue *rq, int maxSize, int numWorkers) {
    rq->m_items = malloc(maxSize * sizeof(Request *));
    if (rq->m_items == NULL) unix_error("malloc failed");
    rq->m_RQCurrentSize = 0;
    rq->m_reqHead       = 0;
    rq->m_reqTail       = 0;
    rq->m_maxRequests   = maxSize;

    rq->udp_mail_head = calloc(numWorkers, sizeof(ping_t *));
    if (rq->udp_mail_head == NULL) unix_error("calloc failed");
    rq->udp_mail_tail = calloc(numWorkers, sizeof(ping_t *));
    if (rq->udp_mail_tail == NULL) unix_error("calloc failed");

    pthread_cond_init(&rq->RQisFull, NULL);
    pthread_cond_init(&rq->RQisEmpty, NULL);
    int rc = pthread_mutex_init(&rq->RQMutex, NULL);
    if (rc != 0) posix_error(rc, "pthread_mutex_init failed");
}

void Req_queue_destroy(Req_Queue *rq, int numWorkers) {
    free(rq->m_items);
    for (int i = 0; i < numWorkers; i++) {
        ping_t *p = rq->udp_mail_head[i];
        while (p != NULL) {
            ping_t *next = p->next;
            free(p);
            p = next;
        }
    }
    free(rq->udp_mail_head);
    free(rq->udp_mail_tail);

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

void rq_enqueue_udp(Req_Queue *rq, int worker_idx, struct sockaddr_in *clientaddr) {
    ping_t *p = malloc(sizeof(ping_t));
    if (p == NULL) unix_error("malloc failed");
    p->addr = *clientaddr;
    p->next = NULL;

    pthread_mutex_lock(&(rq->RQMutex));
    if (rq->udp_mail_tail[worker_idx] != NULL)
        rq->udp_mail_tail[worker_idx]->next = p;
    else
        rq->udp_mail_head[worker_idx] = p;
    rq->udp_mail_tail[worker_idx] = p;
    // broadcast: the ping targets a SPECIFIC worker; signal could wake the
    // wrong one and the target would never learn about its mail
    pthread_cond_broadcast(&(rq->RQisEmpty));
    pthread_mutex_unlock(&(rq->RQMutex));
}

/******************************* ThreadPool functions ******************************/

static void *worker_routine(void *arg);

void thread_pool_init(thread_pool *tp, int MaxReqs, int MaxWorkers, server_log log, int udpfd) {
    tp->TPs_rq = malloc(sizeof(Req_Queue));
    if (tp->TPs_rq == NULL) unix_error("malloc failed");
    Req_queue_init(tp->TPs_rq, MaxReqs, MaxWorkers);

    tp->TPs_log         = log;
    tp->m_numWorkers    = MaxWorkers;
    tp->udpfd           = udpfd;

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

static void *worker_routine(void *arg) {
    worker_arg *wa = (worker_arg *)arg;
    thread_pool *tp = wa->tp;
    int my_id = wa->worker_idx;
    threads_stats stats = tp->m_StatsArray[my_id];
    Req_Queue *rq = tp->TPs_rq;
    free(wa);

    while (1) {
        pthread_mutex_lock(&(rq->RQMutex));

        while(rq->m_RQCurrentSize == 0 && rq->udp_mail_head[my_id] == NULL) {
            pthread_cond_wait(&(rq->RQisEmpty), &(rq->RQMutex));
        }

        if (rq->udp_mail_head[my_id] != NULL) {
            ping_t *p = rq->udp_mail_head[my_id];
            rq->udp_mail_head[my_id] = p->next;
            if (rq->udp_mail_head[my_id] == NULL)
                rq->udp_mail_tail[my_id] = NULL;
            pthread_mutex_unlock(&(rq->RQMutex));

            struct sockaddr_in clientaddr = p->addr;
            free(p);

            char udp_buf[MAXBUF];
            udp_buf[0] = '\0';
            append_thread_log(udp_buf, stats);
            sendto(tp->udpfd, udp_buf, strlen(udp_buf), 0, (struct sockaddr *)&clientaddr, sizeof(clientaddr));
            continue;
        }


        Request *req = rq->m_items[rq->m_reqHead];
        rq->m_reqHead = (rq->m_reqHead + 1) % rq->m_maxRequests;
        rq->m_RQCurrentSize--;
        pthread_cond_signal(&(rq->RQisFull));
        pthread_mutex_unlock(&(rq->RQMutex));


        gettimeofday(&req->m_timestats->task_dispatch, NULL);
        requestHandle(req->m_fd, *req->m_timestats, stats, tp->TPs_log);

        Close(req->m_fd);
        free(req->m_timestats);
        free(req);
    }
    return NULL;
}

void thread_pool_destroy(thread_pool *tp) {
    Req_queue_destroy(tp->TPs_rq, tp->m_numWorkers);
    free(tp->TPs_rq);
    for (int i = 0; i < tp->m_numWorkers; i++) {
        free(tp->m_StatsArray[i]);
    }
    free(tp->m_StatsArray);
    free(tp->m_ThreadIds);
}

/******************************* Main Loop ******************************/

int main(int argc, char *argv[])
{
    int listenfd, udpfd, connfd, tcpPort, udpPort, numThreads, queueSize;
    socklen_t clientlen;
    double debugSleepTime;
    struct sockaddr_in clientaddr;

    getargs(&tcpPort, &udpPort, &numThreads, &queueSize, &debugSleepTime, argc, argv);


    if ((udpfd = UDP_Open(udpPort)) < 0) {
        unix_error("UDP_Open failed");
    }

    server_log log = create_log(debugSleepTime);

    thread_pool tp;
    thread_pool_init(&tp, queueSize, numThreads, log, udpfd);

    listenfd = Open_listenfd(tcpPort);

    // הגדרת משתני select
    fd_set read_set, ready_set;
    FD_ZERO(&read_set);
    FD_SET(listenfd, &read_set);
    FD_SET(udpfd, &read_set);
    int maxfd = (listenfd > udpfd) ? listenfd : udpfd;

    while (1) {
        ready_set = read_set;

        Select(maxfd + 1, &ready_set, NULL, NULL, NULL);

        //UDF request arrives
        if (FD_ISSET(udpfd, &ready_set)) {
            clientlen = sizeof(clientaddr);
            char buf[MAXLINE];
            int n = recvfrom(udpfd, buf, MAXLINE - 1, 0, (SA *)&clientaddr, &clientlen);
            if (n > 0) {
                buf[n] = '\0';
                int target_thread = atoi(buf);
                if (target_thread >= 1 && target_thread <= numThreads) {
                    rq_enqueue_udp(tp.TPs_rq, target_thread - 1, &clientaddr);
                }
            }
        }

        //TCP request arrives
        if (FD_ISSET(listenfd, &ready_set)) {
            clientlen = sizeof(clientaddr);
            connfd = Accept(listenfd, (SA *)&clientaddr, &clientlen);

            Request *req = malloc(sizeof(Request));
            if (req == NULL) unix_error("malloc failed");
            req->m_fd = connfd;
            req->m_timestats = malloc(sizeof(time_stats));
            if (req->m_timestats == NULL) unix_error("malloc failed");

            gettimeofday(&req->m_timestats->task_arrival, NULL);

            rq_enqueue(tp.TPs_rq, req);
        }
    }

    destroy_log(log);
    thread_pool_destroy(&tp);
}
