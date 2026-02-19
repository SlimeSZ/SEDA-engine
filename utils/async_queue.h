#ifndef ASYNC_QUEUEH
#define ASYNC_QUEUEH

#include <pthread.h>
#include <stdint.h>

// use batch pushes more aggresively on timer_fd wake to avoid head/tail lock contentions
typedef struct {
	void **items; 
	int capacity;
	_Atomic int size;

	pthread_mutex_t tail_lock;
	int tail;

	pthread_mutex_t head_lock;
	int head;

	pthread_cond_t not_empty;
	pthread_cond_t not_full;

	_Atomic int shutdown;
} async_queue_t;

async_queue_t *async_queue_init(int capacity);
void async_queue_free(async_queue_t *q);
void async_queue_shutdown(async_queue_t *q); 
int async_queue_push(async_queue_t *q, void *item);
void *async_queue_pop(async_queue_t *q); // can pop with max timeout
/* Non-blocking variants */
int async_queue_try_push(async_queue_t *q, void *item);
void *async_queue_try_pop(async_queue_t *q);
/* Batch (with Non-blocking option) variants */
void async_queue_batch_push(
		async_queue_t *q,
	const void **__restrict__ items,
	const int n,
	const int8_t non_block,
	size_t *__restrict__ in_cnt);
void **async_queue_batch_pop(async_queue_t *q,
                            const size_t n,
                            const int8_t non_block,
                            size_t *__restrict__ out_cnt);
/* State Checks */
int async_queue_empty(async_queue_t *q);
int async_queue_full(async_queue_t *q);
int async_queue_is_shutdown(async_queue_t *q);



#endif

