#ifndef WORKER_POOL_H
#define WORKER_POOL_H

#include <pthread.h>
#include "async_queue.h"

typedef struct {
	void *task;				// actual task inside scheduler 
	uint64_t scheduled_run_ns;		// which tick this execution corresponds to 
} ready_entry_t;

/* Worker function ptr, executes entry based on opaque context */
typedef void (*worker_entry_fn_t)(ready_entry_t *entry, void *ctx);

typedef struct {
	_Atomic uint64_t *completion_map;	// scheduler's completion bitmap
	int completion_eventfd;			// schedulers completion wake fd 
	async_queue_t *output_queue;		// schedulers output_queue
} worker_ctx_t;

typedef struct {
	pthread_t *workers;
	size_t size;
	_Atomic size_t num_free;
	_Atomic size_t num_working;

	async_queue_t *ready_queue;		// borrowed from scheduler not owned 
	worker_entry_fn_t entry_fn;		// all workers call this fn first upon task execution
	worker_ctx_t ctx;			// all workers refer to the same context struct

	_Atomic int shutdown;
} worker_pool_t;

/* API */
worker_pool_t *worker_pool_init(size_t num_workers,
	                        async_queue_t *ready_queue,
                                worker_entry_fn_t entry_fn,
                                worker_ctx_t ctx);
void worker_pool_shutdown(worker_pool_t *pool);

#endif
