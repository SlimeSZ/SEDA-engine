#ifndef HEAP_H
#define HEAP_H

#include "../producers/scheduler/scheduler.h"

#define PARENT(idx) (idx - 1) / 2
#define LEFT(idx) 2 * idx + 1
#define RIGHT(idx) 2 * idx + 2

static inline void heap_swap(scheduler_entry_t *a, scheduler_entry_t *b) {
	scheduler_entry_t tmp = *a;
	*a = *b;
	*b = tmp;
}

static inline void heapify_up(scheduler_t *s, size_t idx) {
	while (idx > 0) {
		size_t parent = PARENT(idx);
		if (s->heap[idx].next_run_ns >= s->heap[parent].next_run_ns)
			break;
		heap_swap(&s->heap[idx], &s->heap[parent]);
		idx = parent;
	}	
}

static inline void heapify_down(scheduler_t *s, size_t idx) {
	size_t left, right, smallest;
	while (1) {
		left = LEFT(idx);
		right = RIGHT(idx);
		smallest = idx;

		if (left < s->size && s->heap[left].next_run_ns < s->heap[smallest].next_run_ns)
			smallest = left;
		if (right < s->size && s->heap[right].next_run_ns < s->heap[smallest].next_run_ns)
			smallest = right;

		if (smallest == idx) 
			break;

		heap_swap(&s->heap[idx], &s->heap[smallest]);
		idx = smallest;
	}
}

static inline int heap_push(scheduler_t *s, scheduler_entry_t *task) {
	if (s->size >= s->capacity) return -1;
	s->heap[s->size] = *task;
	heapify_up(s, s->size);
	s->size++;
	return 0;
}

static inline scheduler_entry_t heap_pop(scheduler_t *s) {
	scheduler_entry_t task = s->heap[0];
	s->size--;
	if (s->size > 0) {
		s->heap[0] = s->heap[s->size];
		heapify_down(s, 0);
	}
	return task;
}

static inline scheduler_entry_t *heap_peek(scheduler_t *s) {
	if (s->size == 0) return NULL;
	return &s->heap[0];
}

static inline int heap_empty(const scheduler_t *s) { return s->size == 0; }
static inline int heap_full(const scheduler_t *s) { return s->size >= s->capacity; }

#endif
