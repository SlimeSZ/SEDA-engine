#include "worker_pool.h"
#include "../scheduler/scheduler.h"
#include <stdlib.h>
#include <stddef.h>
#include <stdatomic.h>
#include <sys/epoll.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>

/*
 * Workers sit idle here as soon as pool is initialized & between tasks, 
 * woken by new task in ready queue, upon which workers execute, result 
 * is available immediately in this function, however, as of now, we allow 
 * the calling function to decide how it is used (i.e is it returned directly to caller,
 * pushed to output queue, etc..). Reason being; this is a routine for pthread_create(),
 * thus the length to which we can handle the result in a robust manner directly within 
 * this fn is inherently limited. This may however, change in the future as I am working 
 * on adding for example, the option to NOT push the result to the output queue if the 
 * user chooses via a flag passed through worker_ctx_t, so keep this in mind
*/
static void *worker_thread_routine(void *arg) {
	worker_pool_t *pool = (worker_pool_t*)arg;

	while (!atomic_load(&pool->shutdown)) {
		task_t *task = async_queue_try_pop(pool->ctx.ready_queue);
		if (!task) {
			task = async_queue_pop(pool->ctx.ready_queue);  
			if (!task) // NULL means shutdown 
				break;
        	}
		atomic_fetch_add(&pool->num_working, 1);
		atomic_fetch_sub(&pool->num_free, 1);

		pool->ctx.entry_fn(task, &pool->ctx);

		atomic_fetch_sub(&pool->num_working, 1);
        	atomic_fetch_add(&pool->num_free, 1); 
	}	

	return NULL;
}


worker_pool_t *worker_pool_init(
	size_t size,
	worker_ctx_t ctx
) {
	if (size == 0 || size > MAX_WORKERS_PER_SCHEDULER)
		return NULL;
	
	// init pool	
	worker_pool_t *pool = calloc(1, sizeof(worker_pool_t));
	if (!pool)
		return NULL;

	pool->workers = calloc(size, sizeof(pthread_t));
	if (!pool->workers) {
		free(pool);
		return NULL;
	}
	pool->size = size;
	atomic_store(&pool->num_free, size);
	atomic_store(&pool->num_working, 0);
	pool->ctx = ctx;
	atomic_store(&pool->shutdown, 0);

	// create N threads & send to sit idle until task's ready
	for (size_t i = 0; i < size; i++) {
		if (pthread_create(&pool->workers[i], NULL, worker_thread_routine, pool)
			!= 0) {
			atomic_store(&pool->shutdown, 1);
			for (size_t j = 0; j < i; j++) 
				pthread_join(pool->workers[j], NULL);
			free(pool->workers);
			free(pool);
			return NULL;
		}
	}
	return pool;
}

/*
 * Pool shutdown procedure involves the assurance that both below scenarios
 * are accounted for:
 * a) worker(s) may be mid-task-execution 	(pthread_join handles this)
 * b) worker(s) may be blocking in routine, waiting for a task to be pushed
 * 	(broadcast to those blocking in pop(), or batch_pop() via 'not-empty'
 * 	condition from async_queue_shutdown, upon which said workers wake, 
 * 	leading to graceful shutdown)
 *
 * IMPORTANT: ensure async_queue_shutdown(ready_queue) called prior to calling this fn
*/
void worker_pool_shutdown(worker_pool_t *pool) {
	if (!pool)
		return;
	atomic_store(&pool->shutdown, 1);
	
	// allow idle, and more importantly mid-execution workers to complete
	for (size_t i = 0; i < pool->size; i++) 
		pthread_join(pool->workers[i], NULL);
	free(pool->workers);
	free(pool);
}
