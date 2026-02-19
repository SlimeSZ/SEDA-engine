#ifndef WORKER_POOLH
#define WORKER_POOLH

#include <pthread.h>
#include <stdint.h>
#include "async_queue.h"

#define MAX_WORKERS_PER_SCHEDULER 448 		// max worker threads in a scheduler 
// #define MAX_TASKS_PER_WORKERS // max logical tasks in each thread 

typedef void *(*worker_entry_fn_t)(void *task, void *void_ctx);

typedef struct {
	_Atomic uint64_t *completion_map;	// borrowed from scheduler 
	int completion_eventfd;			// borrowed from scheduler  
	async_queue_t *output_queue;		// borrowed from scheduler
	async_queue_t *ready_queue;		// borrowed from scheduler
	worker_entry_fn_t entry_fn;
} worker_ctx_t;

typedef struct {
	pthread_t *workers;
	size_t size;
	_Atomic size_t num_free;
	_Atomic size_t num_working;

	worker_ctx_t ctx;

	int shutdown_eventfd; 		
	_Atomic int shutdown;
} worker_pool_t;

worker_pool_t *worker_pool_init(
	size_t size,
	worker_ctx_t ctx
);
void worker_pool_shutdown(worker_pool_t *pool);

#endif

