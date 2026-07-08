#ifndef SERVER_LOG_H
#define SERVER_LOG_H

#include <sys/time.h>

typedef struct Server_Log* server_log;

struct Threads_stats;
struct Time_stats;
// Creates a new server log instance
server_log create_log(double debug_sec);

// Destroys and frees the log
void destroy_log(server_log log);

// Returns the log contents as a string (null-terminated)
// NOTE: caller is responsible for freeing dst
int get_log(server_log log, char** dst, struct Time_stats* tm_stats);

// Appends a new entry to the log
void add_to_log(server_log log, struct Threads_stats* t_stats, struct Time_stats* tm_stats);


#endif // SERVER_LOG_H
