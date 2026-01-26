#ifndef SCHEDULER_H
#define SCHEDULER_H

#include <stdint.h>
#include <pthread.h>
#include "../../utils/async_queue.h"

#define MAX_SCHEDULER_THREADS 
#define MAX_TASKS 10

typedef struct {
	void (*task_fn)(void *arg);	// task function to execute
	void *arg;			// parameters
	uint64_t interval_ns;		// polling interval (nanoseconds)
	uint64_t next_run_ns;		// internal tracking (when it should run next)
} scheduled_task_t;

typedef struct {
	scheduled_task_t *heap;		// min-heap for earlier task guarantee without linear scan
	size_t size;
	size_t capacity;

	async_queue_t *output_queue;	// where tasks push outputs
	pthread_t timer_thread; 	// scheduling manager

	int epfd;			// epoll instance for blocking
	int tfd; 			// timerfd (kernel-managed wakeup timer)

	_Atomic int shutdown;
} scheduler_t;

#endif
