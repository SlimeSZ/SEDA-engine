#include "async_queue.h"
#include <bits/time.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>
#include <time.h>
#include <errno.h>

/* Init/Free Utils */
static inline void free_internals(async_queue_t *q) {
	pthread_mutex_destroy(&q->tail_lock);
	pthread_mutex_destroy(&q->head_lock);
	free(q->items);
	free(q);
}
static void async_queue_shutdown(async_queue_t *q) {
    if (!q) return;

    atomic_store(&q->shutdown, 1);

    pthread_cond_broadcast(&q->not_full);
    pthread_cond_broadcast(&q->not_empty);
}

/* Inline Queue-state Utils */
static inline __attribute__((always_inline))
int async_queue_empty(async_queue_t *q) {
    return atomic_load(&q->size) == 0;
}

static inline __attribute__((always_inline))
int async_queue_full(async_queue_t *q) {
    return atomic_load(&q->size) == q->capacity;
}

static inline __attribute__((always_inline)) 
int async_queue_is_shutdown(async_queue_t *q) {
	return atomic_load(&q->shutdown);
}

static inline __attribute__((always_inline))
void push(async_queue_t *q, void *item) {
	q->items[q->tail] = item;
	q->tail = (q->tail + 1) % q->capacity;
	atomic_fetch_add(&q->size, 1);
}

static inline __attribute__((always_inline))
void *pop(async_queue_t *q) {
	void *item = q->items[q->head];
	q->head = (q->head + 1) % q->capacity;
	atomic_fetch_sub(&q->size, 1);
	return item;
}

static inline void compute_abs_timeout(uint64_t timeout_ns, struct timespec *ts) {
	struct timespec now;
	clock_gettime(CLOCK_MONOTONIC, &now);
	ts->tv_sec = now.tv_sec + (timeout_ns / 1000000000ULL);
	ts->tv_nsec = now.tv_nsec + (timeout_ns % 1000000000ULL);
	if (ts->tv_nsec >= 1000000000L) {
		ts->tv_sec  += 1;
		ts->tv_nsec -= 1000000000L;
	}
}

/* Core functionality */
static async_queue_t *async_queue_init(int size) {
	if (size <= 0) return NULL;

	async_queue_t *q = malloc(sizeof(async_queue_t));
	if (!q) return NULL;

	q->items = malloc(sizeof(void*) * size);
	if (!q->items) {
		free(q);
		return NULL;
	}
	memset(q->items, 0, sizeof(void*) * size);
	
	q->capacity = size;
	q->tail = 0;
	q->head = 0;
	atomic_init(&q->size, 0);
	atomic_init(&q->shutdown, 0);

	if (pthread_mutex_init(&q->tail_lock, NULL) != 0) {
		free(q->items);
		free(q);
		return NULL;
	}
	if (pthread_mutex_init(&q->head_lock, NULL) != 0) {
		pthread_mutex_destroy(&q->tail_lock);
		free(q->items);
		free(q);
		return NULL;
	}

	pthread_condattr_t cond_attr;
	if (pthread_condattr_init(&cond_attr) != 0) {
		free_internals(q);
		return NULL;
	}
	if (pthread_condattr_setclock(&cond_attr, CLOCK_MONOTONIC) != 0) {
		pthread_condattr_destroy(&cond_attr);
		free_internals(q);
		return NULL;
	}
	if (pthread_cond_init(&q->not_empty, &cond_attr) != 0 ||
	   pthread_cond_init(&q->not_full, &cond_attr) != 0) {
		pthread_condattr_destroy(&cond_attr);
		free_internals(q);
		return NULL;
	}
	
	pthread_condattr_destroy(&cond_attr);
	return q;
}

static void async_queue_free(async_queue_t *q) {
    if (!q) return;
    async_queue_shutdown(q);
    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
    free_internals(q);
}

/*
 * - Blocks while queue is full, realeasing producer push lock (tail_lock); 
 * consumers must signal via 'not_full' upon pop() to wake producer
 * - Signals 'not_empty' upon push to wake any consumers 
 * 
 * returns 0 on successm -1 on queue shutdown or invalid parameters
 */
static int async_queue_push(async_queue_t *q, void *item) {
	if (!q || !item) return -1;
	pthread_mutex_lock(&q->tail_lock);

	while (async_queue_full(q) && !async_queue_is_shutdown(q))
		pthread_cond_wait(&q->not_full, &q->tail_lock);

	if (async_queue_is_shutdown(q)) {
		pthread_mutex_unlock(&q->tail_lock);
		return -1;
	}

	push(q, item);
	pthread_cond_signal(&q->not_empty);

	pthread_mutex_unlock(&q->tail_lock);
	return 0;
}

/*
 * - Null 'timeout_ns' param causes indefinite block while queue empty, releasing consumer pop lock (head_lock) 
 *   upon doing so, producer must signal consumer to pop() via 'not_empty' cond upon a push(). 
 * - Signals 'not_full' upon pop to indicate to any producers it is ready for more items 
 *
 * - Otherwise NonNull 'timeout_ns' param blocks for specified timeout 
 *
 * - returns (void) popped item on success, NULL on queue shutdown or invalid parameters
*/
static void *async_queue_pop(async_queue_t *q, uint64_t *timeout_ns) {
	if (!q) return NULL;
	pthread_mutex_lock(&q->head_lock);

	while (async_queue_empty(q) && !async_queue_is_shutdown(q)) {
		if (timeout_ns != NULL) {
			struct timespec abs_timeout;
			compute_abs_timeout(*timeout_ns, &abs_timeout);
			int rc = pthread_cond_timedwait(&q->not_empty, &q->head_lock, &abs_timeout);
			if (rc == ETIMEDOUT) {
				pthread_mutex_unlock(&q->head_lock);
				return NULL;
			}
		} else {
			pthread_cond_wait(&q->not_empty, &q->head_lock);
		}
	}

	if (async_queue_is_shutdown(q)) {
		pthread_mutex_unlock(&q->head_lock);
		return NULL;
	}
	
	void *item = pop(q);
	pthread_cond_signal(&q->not_full);

	pthread_mutex_unlock(&q->head_lock);
	return item;
}

/*
 * Non-blocking consumer/producer push/pops 
 *
 * returns 0 on success, -1 if queue full/empty, shutdown, or invalid parameters
*/
static int async_queue_try_push(async_queue_t *q, void *item) {
	if (!q || !item) return -1;
	pthread_mutex_lock(&q->tail_lock);

	if (async_queue_is_shutdown(q) || async_queue_full(q)) {
		pthread_mutex_unlock(&q->tail_lock);
		return -1;
	}

	push(q, item);
	pthread_cond_signal(&q->not_empty);

	pthread_mutex_unlock(&q->tail_lock);
	return 0;
}
static void *async_queue_try_pop(async_queue_t *q) {
	if (!q) return NULL;
	pthread_mutex_lock(&q->head_lock);

	if (async_queue_is_shutdown(q) || async_queue_empty(q)) {
		pthread_mutex_unlock(&q->head_lock);
		return NULL;
	}

	void *item = pop(q); 
	pthread_cond_signal(&q->not_full);

	pthread_mutex_unlock(&q->head_lock);
	return item;
}

// THESE COMMENTS ARE NOT ACCURATE WHATSOEVER
/* - Constraints: 
 * 	* N < queue capacity - queue size 
 *
 * returns positive int indicating total successful pushes, 0 if non_block enabled & queue was full, 
 * or -1 on failure
 * 	* If full success: ret == in_cnt  
 * 	* If partial success: 0 < ret < in_cnt 
*/
__attribute__((always_inline, hot, nonnull))
int async_queue_batch_push(async_queue_t *q, 
                           const void **__restrict__ items, 
                           const int n, 
                           const int8_t non_block,
			   size_t *__restrict__ in_cnt) {
	if (n < 1) return -1;
	*in_cnt = 0;
	pthread_mutex_lock(&q->tail_lock);

	if (!non_block) {
		while (!async_queue_is_shutdown(q) && async_queue_full(q))
			pthread_cond_wait(&q->not_full, &q->head_lock);
	} else {
		if (async_queue_is_shutdown(q) || async_queue_full(q)) {
			pthread_mutex_unlock(&q->tail_lock);
			return -1;
		}
	}
	if (async_queue_is_shutdown(q)) {
		pthread_mutex_unlock(&q->tail_lock);
		return NULL;
	}

	const size_t available = q->capacity - atomic_load(&q->size);
	if (available == 0) {
		pthread_mutex_unlock(&q->tail_lock);
		return 0;
	}

	const size_t to_push = (available < n) ? available : n;
	const size_t till_wrap = q->capacity - q->tail;

	const size_t first_chunk = (to_push < till_wrap) ? to_push : till_wrap;
	const size_t second_chunk = to_push - first_chunk;

	memcpy(&q->items[q->tail], items, first_chunk * sizeof(void*));
	if (second_chunk)
		memcpy(&q->items[0], items + first_chunk, second_chunk * sizeof(void*));

	q->tail = (q->tail + to_push) % q->capacity;
	atomic_fetch_add(&q->size, 1);
	*in_cnt = to_push;

	pthread_cond_signal(&q->not_empty);

	pthread_mutex_unlock(&q->tail_lock);
	return (int)to_push;
}

__attribute__((always_inline, hot, nonnull))
void **async_queue_batch_pop(async_queue_t *q,
                            const size_t n,
                            const int8_t non_block,
                            size_t *__restrict__ out_cnt) {
	if (n < 1) return NULL;
	*out_cnt = 0;
	pthread_mutex_lock(&q->head_lock);

	if (!non_block) {
		while (!async_queue_is_shutdown(q) && async_queue_empty(q)) 
			pthread_cond_wait(&q->not_empty, &q->head_lock);
	}

	if (async_queue_is_shutdown(q)) {
		pthread_mutex_unlock(&q->head_lock);
		return NULL;
	}

	const size_t available = atomic_load(&q->size);
	const size_t take = available < n ? available : n;

	void **batch = aligned_alloc(32, sizeof(void*) * take);
	if (!batch) {
		pthread_mutex_unlock(&q->head_lock);
		return NULL;
	}

	const size_t till_wrap = q->capacity - q->head; 
	const size_t first_chunk = (take < till_wrap) ? take : till_wrap; 
	const size_t second_chunk = take - first_chunk;

	memcpy(batch, &q->items[q->head], first_chunk * sizeof(void));
	if (second_chunk)
		memcpy(batch + first_chunk, &q->items[0], second_chunk * sizeof(void*));

	q->head = (q->head + take) % q->capacity;
	atomic_fetch_sub(&q->size, 1);

	pthread_cond_signal(&q->not_full);

	pthread_mutex_unlock(&q->head_lock);
	
	*out_cnt = take;
	return batch;
}

int main(void) {

	return 0;
}
