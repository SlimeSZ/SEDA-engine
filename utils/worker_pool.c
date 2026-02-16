#include "worker_pool.h"
#include <stdlib.h>
#include <stddef.h>
#include <stdatomic.h>

/*
 * Make design option to possibly only wake if batch available (use batch_pop),
 * do something with try_pop?
*/
static void *worker_thread_routine(void *arg) {
	worker_pool_t *pool = (worker_pool_t *)arg;

	while (!atomic_load(&pool->shutdown)) {
		// block until scheduler dispatches a task into the "ready-to-exec-queue" 
		ready_entry_t *entry = async_queue_pop(pool->ready_queue, NULL);
		if (!entry || async_queue_is_shutdown(pool->ready_queue)) continue;

		atomic_fetch_sub(&pool->num_free, 1);
		atomic_fetch_add(&pool->num_working, 1);

		pool->entry_fn(entry, &pool->ctx);

		free(entry);

		atomic_fetch_add(&pool->num_free, 1);
		atomic_fetch_sub(&pool->num_working, 1);
	}
	return NULL;
}

worker_pool_t *worker_pool_init(size_t num_workers,
	                        async_queue_t *ready_queue,
                                worker_entry_fn_t entry_fn,
                                worker_ctx_t ctx) {
	if (num_workers == 0 || num_workers > MAX_TASKS) 
		return NULL;
	worker_pool_t *pool = calloc(1, sizeof(worker_pool_t));
	if (!pool) 
		return NULL;
	
	pool->workers = calloc(num_workers, sizeof(pthread_t));
	if (!pool->workers) {
		free(pool);
		return NULL;
	}
	pool->size = num_workers;
	atomic_store(&pool->num_free, num_workers);
	atomic_store(&pool->num_working, 0);
	pool->ready_queue = ready_queue;
	pool->entry_fn = entry_fn;
	pool->ctx = ctx;

	// spawn workers 
	for (size_t i = 0; i < num_workers; i++) {
		if (pthread_create(&pool->workers[i], NULL, worker_thread_routine, pool) != 0) {
			atomic_store(&pool->shutdown, 1);
			for (size_t j = 0; j < i; j++) {
				pthread_join(pool->workers[j], NULL);
			}
			free(pool->workers);
			free(pool);
			return NULL;
		}
	}
	return pool;
}


void worker_pool_shutdown(worker_pool_t *pool) {
	if (!pool) return;

	atomic_store(&pool->shutdown, 1);

	for (size_t i = 0; i < pool->size; i++) 
		pthread_join(pool->workers[i], NULL);

	free(pool->workers);
	free(pool);
}

int main(void) {



	return 0;
}
