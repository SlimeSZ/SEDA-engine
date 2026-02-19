#include <bits/time.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>
#include "async_queue.h"
#include <unistd.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>

#define MAX_TASKS 400

static inline void free_internals(async_queue_t *q) {
	pthread_mutex_destroy(&q->tail_lock);
	pthread_mutex_destroy(&q->head_lock);
	free(q->items);
	free(q);
}

static inline __attribute__((always_inline, nonnull))
void push(async_queue_t *q, void *item) {
	q->items[q->tail] = item;
	q->tail = (q->tail + 1) % q->capacity;
	atomic_fetch_add(&q->size, 1);
}

static inline __attribute__((always_inline, nonnull))
void *pop(async_queue_t *q) {
	void *item = q->items[q->head];
	q->head = (q->head + 1) % q->capacity;
	atomic_fetch_sub(&q->size, 1);
	return item;
}

static inline __attribute__((always_inline)) 
void compute_abs_timeout(uint64_t timeout_ns, struct timespec *ts) {
	struct timespec now;
	clock_gettime(CLOCK_MONOTONIC, &now);
	ts->tv_sec = now.tv_sec + (timeout_ns / 1000000000ULL);
	ts->tv_nsec = now.tv_nsec + (timeout_ns % 1000000000ULL);
	if (ts->tv_nsec >= 1000000000L) {
		ts->tv_sec  += 1;
		ts->tv_nsec -= 1000000000L;
	}
}

static inline __attribute__((always_inline, hot, nonnull))
void async_queue_print(async_queue_t *q) {
	pthread_mutex_lock(&q->head_lock);
	pthread_mutex_lock(&q->tail_lock);

	const size_t n = (size_t)atomic_load(&q->size);
	const size_t cap = (size_t)q->capacity;
	size_t idx = (size_t)q->head;
	
	for (size_t i = 0; i < n; ++i) {
		void *item = q->items[idx];
		printf(" [%zu] %p\n", i, item);
		idx++;
		if (idx == cap) idx = 0;
	}

	pthread_mutex_unlock(&q->tail_lock);
	pthread_mutex_unlock(&q->head_lock);
}

/* USER API  */

inline __attribute__((always_inline, nonnull))
int async_queue_empty(async_queue_t *q) {
	return atomic_load(&q->size) == 0;
}

inline __attribute__((always_inline, nonnull))
int async_queue_full(async_queue_t *q) {
	return atomic_load(&q->size) == q->capacity;
}

inline __attribute__((always_inline, nonnull))
int async_queue_is_shutdown(async_queue_t *q) {
	return atomic_load(&q->shutdown);
}

async_queue_t *async_queue_init(int capacity) {
	if (capacity <= 0 || capacity >= MAX_TASKS)
		return NULL;
	
	async_queue_t *q = malloc(sizeof(async_queue_t));
	if (!q) 
		return NULL;
	q->items = calloc(capacity, sizeof(void*));
	if (!q->items) {
		free(q);
		return NULL;
	}
	
	q->capacity = capacity;
	q->head = 0;
	q->tail = 0;
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
	if (pthread_cond_init(&q->not_empty, &cond_attr) != 0) {
		pthread_condattr_destroy(&cond_attr);
		free_internals(q);
		return NULL;	
	}
	if (pthread_cond_init(&q->not_full, &cond_attr) != 0) {
		pthread_cond_destroy(&q->not_empty);
		pthread_condattr_destroy(&cond_attr);
		free_internals(q);
		return NULL;

	}
	pthread_condattr_destroy(&cond_attr);
	return q;
}

void async_queue_free(async_queue_t *q) {
	pthread_cond_destroy(&q->not_empty);
	pthread_cond_destroy(&q->not_full);
	free_internals(q);
}

void async_queue_shutdown(async_queue_t *q) {
	if (!q) 
		return;
	atomic_store_explicit(&q->shutdown, 1, memory_order_release);
	
	pthread_cond_broadcast(&q->not_full);
	pthread_cond_broadcast(&q->not_empty);
}

int async_queue_push(async_queue_t *q, void *item) {
	if (!q || !item)
		return -1;
	pthread_mutex_lock(&q->tail_lock);
	while (async_queue_full(q) && !async_queue_is_shutdown(q)) {
		printf("[push()] queue full... waiting for pop()\n");
		pthread_cond_wait(&q->not_full, &q->tail_lock);
	}
	printf(
		"[push()] awaken, queue has space\n"
	);
	push(q, item);
	
	int now_sz = atomic_load(&q->size);
	printf("[push()] PUSHED! - queue size now: %d\n", now_sz);

	pthread_cond_signal(&q->not_empty);

	pthread_mutex_unlock(&q->tail_lock);
	return 0;
}

void *async_queue_pop(async_queue_t *q) {
	if (!q) 
		return NULL;
	pthread_mutex_lock(&q->head_lock);
	while (async_queue_empty(q) && !async_queue_is_shutdown(q)) {
		printf("[pop()] queue empty... waiting for push()\n");
		pthread_cond_wait(&q->not_empty, &q->head_lock);
	}
	if (async_queue_is_shutdown(q)) {
		pthread_mutex_unlock(&q->head_lock);
		return NULL;
	}
	printf(
		"[pop()] awaken, queue has item\n"
	);

	void *item = pop(q);

	int now_sz = atomic_load(&q->size);
	printf("[pop()] POPPED! - queue size now: %d\n", now_sz);

	pthread_cond_signal(&q->not_full);

	pthread_mutex_unlock(&q->head_lock);
	return item;
}

int async_queue_try_push(async_queue_t *q, void *item) {
	if (!q || !item)
		return -1;
	pthread_mutex_lock(&q->tail_lock);
	
	if (async_queue_is_shutdown(q) || async_queue_full(q)) {
		int new_sz = atomic_load(&q->size);
		printf("[try_push()] queue full (size=%d) - RETURNING\n",
			new_sz);
		pthread_mutex_unlock(&q->tail_lock);
		return -1;
	}

	if (async_queue_is_shutdown(q)) {
		pthread_mutex_unlock(&q->tail_lock);
		return -1;
	}
	
	push(q, item);
	int now_sz = atomic_load(&q->size);
	printf("[try_push()] PUSHED! - queue size now: %d\n", now_sz);

	pthread_cond_signal(&q->not_empty);

	pthread_mutex_unlock(&q->tail_lock);
	return 0;
}

void *async_queue_try_pop(async_queue_t *q) {
	if (!q)
		return NULL;
	pthread_mutex_lock(&q->head_lock);

	if (async_queue_is_shutdown(q) || async_queue_empty(q)) {
		printf("[try_pop()] queue empty - RETURNING\n");
		pthread_mutex_unlock(&q->head_lock);
		return NULL;
	}
	
	if (async_queue_is_shutdown(q)) {
		pthread_mutex_unlock(&q->head_lock);
		return NULL;
	}

	void *item = pop(q);
	int now_sz = atomic_load(&q->size);
	printf("[try_push()] POPPED! - queue size now: %d\n", now_sz);

	pthread_cond_signal(&q->not_full);

	pthread_mutex_unlock(&q->head_lock);
	return item;
}

void async_queue_batch_push(
	async_queue_t *q,
	const void **__restrict__ items,
	const int n,
	const int8_t non_block,
	size_t *__restrict__ in_cnt
) {
	if (!q || !items || n < 2 || !in_cnt)
		return;
	
	*in_cnt = 0;
	pthread_mutex_lock(&q->tail_lock);

	if (non_block) {
		while (async_queue_is_shutdown(q) || async_queue_full(q)) {
			int new_sz = atomic_load(&q->size);
			printf("[batch_push()] queue full (size=%d) - returning\n",
				new_sz);
			pthread_mutex_unlock(&q->tail_lock);
			return;
		}
	} else {
		while (!async_queue_is_shutdown(q) && async_queue_full(q)) {
			printf("[batch_push()] queue full - waiting for a pop\n");
			pthread_cond_wait(&q->not_full, &q->tail_lock);
		}
		printf("[batch_push()] awaken, queue has space\n");
	}

	if (async_queue_is_shutdown(q)) {
		pthread_mutex_unlock(&q->tail_lock);
		return;
	}

	const size_t space = q->capacity - atomic_load(&q->size);
	if (space == 0) {
		printf(
		"[batch_push()] Need to push %d items but space in queue is %zu - RETURNING\n",
		n, space
		);
		pthread_mutex_unlock(&q->tail_lock);
		return;
	}

	const size_t to_push = space > n ? n : space;

	size_t pushed = 0; 
	for (size_t i = 0; i < to_push; ++i) {
		push(q, (void*)items[i]);
		++pushed;
	}
	*in_cnt = pushed;

	pthread_cond_broadcast(&q->not_empty);
	pthread_mutex_unlock(&q->tail_lock);
}

void **async_queue_batch_pop(
	async_queue_t *q,
	const size_t n,
	const int8_t non_block,
	size_t *__restrict__ out_cnt
) {
	if (n < 2) 
		return NULL;
	*out_cnt = 0;
	pthread_mutex_lock(&q->head_lock);

	if (non_block) {
		if (async_queue_is_shutdown(q) || async_queue_empty(q)) {
			printf("[batch_pop()] queue empty - returning\n");
			pthread_mutex_unlock(&q->head_lock);
			return NULL;
		}
	} else {
		while (async_queue_empty(q) && !async_queue_is_shutdown(q)) {
			printf("[batch_pop()] queue empty - waiting for push()\n");
			pthread_cond_wait(&q->not_empty, &q->head_lock);
		}
		printf("[batch_pop()] awaken - queue has item\n");
	}

	if (async_queue_is_shutdown(q)) {
		pthread_mutex_unlock(&q->head_lock);
		return NULL;
	}

	const size_t available = atomic_load(&q->size);
	if (available == 0) {
		pthread_mutex_unlock(&q->head_lock);
		return NULL;
	}
	const size_t to_pop = n > available ? available : n;
	
	void **items = malloc(sizeof(void*) * to_pop);

	size_t popped = 0;
	for (size_t i = 0; i < to_pop; i++) {
		items[i] = pop(q);
		++popped;	
	}

	*out_cnt = popped;
	pthread_cond_broadcast(&q->not_full);
	pthread_mutex_unlock(&q->head_lock);
	return items;
}

