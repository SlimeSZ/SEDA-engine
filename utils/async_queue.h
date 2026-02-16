#ifndef ASYNC_QUEUEH
#define ASYNC_QUEUEH

#include <stdint.h>
#include <pthread.h>

typedef struct {
	void **items;
	int capacity;

	pthread_mutex_t tail_lock;		// lock split: Producer pushes to tail  
	int tail;
	
	pthread_mutex_t head_lock;		// lock split: Consumer pops from head 
	int head;

	_Atomic int size;
	pthread_cond_t not_empty;
	pthread_cond_t not_full;
	
	_Atomic int shutdown;
} async_queue_t;

/* Core functioanlity */
async_queue_t *async_queue_init(int size, int capacity);
void async_queue_free(async_queue_t *q);
void async_queue_shutdown(async_queue_t *q); 
int async_queue_push(async_queue_t *q, void *item);
void *async_queue_pop(async_queue_t *q, uint64_t *timeout_ns); // can pop with max timeout
/* Non-blocking variants */
int async_queue_try_push(async_queue_t *q, void *item);
void *async_queue_try_pop(async_queue_t *q);
/* Batch (with Non-blocking option) variants */
__attribute__((always_inline, hot, nonnull))
int async_queue_batch_push(async_queue_t *q, 
                           const void **__restrict__ items, 
                           const int n, 
                           const int8_t non_block,
			   size_t *__restrict__ in_cnt);
__attribute__((always_inline, hot, nonnull))
void **async_queue_batch_pop(async_queue_t *q,
                            const size_t n,
                            const int8_t non_block,
                            size_t *__restrict__ out_cnt);
/* State Checks */
int async_queue_empty(async_queue_t *q);
int async_queue_full(async_queue_t *q);
int async_queue_is_shutdown(async_queue_t *q);

/*		TODO;
Priority awareness – allow items to carry a priority hint so consumers can pop urgent tasks first
Wait-free ring buffer option – optionally replace locks with atomic head/tail indices for low-latency concurrent access
Graceful shutdown/drain – add functionality such that shutdown waits for all queued items to be processed before exiting
*/

#endif
