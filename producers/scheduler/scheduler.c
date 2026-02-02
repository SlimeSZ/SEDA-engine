#include "scheduler.h"
#include "../../utils/heap.h"
#include <bits/time.h>
#include <bits/types/struct_itimerspec.h>
#include <stdint.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdatomic.h>
#include <string.h>
#include <sys/epoll.h>
#include <time.h>
#include <unistd.h>
#include <sys/timerfd.h>
#include <sys/eventfd.h>
#include <errno.h>
#include <stdio.h>

/* Utilities */
static inline __attribute__((always_inline, hot))
uint64_t get_monotonic_ns(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static void handle_shutdown(scheduler_t *s) {
	/* Set scheduler shutdown flag */
	atomic_store(&s->shutdown, 1);

	/* Drain worker pool, preventing new executions and waiting for current to stop,
	   before draining ready queue */
	ready_entry_t *pending_task;
	while ((pending_task = async_queue_try_pop(s->ready_queue)) != NULL) {
		if (pending_task->task) 
			atomic_store(&((task_t*)pending_task)->state, TASK_CANCELLED);
		free(pending_task);
	}
	while (atomic_load(&s->pool->num_working) > 0)
		sched_yield();
	worker_pool_shutdown(s->pool);
	async_queue_shutdown(s->ready_queue);
	async_queue_free(s->ready_queue);

	/* Drain cntrl queue */
	scheduler_ctrl_payload *ctrl_pending;
	while ((ctrl_pending = async_queue_try_pop(s->ctrl_queue)) != NULL) 
		free(ctrl_pending);
	async_queue_shutdown(s->ctrl_queue);
	async_queue_free(s->ctrl_queue);

	// user-owned s->output_queue, no free() here

	/* Cleanup task-heap */
	for (size_t i = 0; i < s->size; i++) {
		task_t *task = s->heap[i].task; // user-owned task, no free() here
		if (!task) continue;
		
		task_state_t t_state = atomic_load(&task->state);
		if (t_state == TASK_SCHEDULED)
			atomic_store(&task->state, TASK_CANCELLED);
	}
	free(s->heap);

	/* Close all fds */
	epoll_ctl(s->epoll_fd, EPOLL_CTL_DEL, s->timer_fd, NULL);
	epoll_ctl(s->epoll_fd, EPOLL_CTL_DEL, s->ctrl_eventfd, NULL);
	epoll_ctl(s->epoll_fd, EPOLL_CTL_DEL, s->completion_eventfd, NULL);
	close(s->timer_fd);
	close(s->ctrl_eventfd);
	close(s->completion_eventfd);
	close(s->epoll_fd);

	free(s);
}

static void handle_ctrl_request(scheduler_t *s) {
	uint64_t x;
	read(s->ctrl_eventfd, &x, sizeof(x));

	scheduler_ctrl_payload *payload;
	while ((payload = async_queue_try_pop(s->ctrl_queue)) != NULL) {
		switch (payload->op) {
			case SCHED_ADD:
				return;
			case SCHED_REMOVE:
				return;
			case SCHED_PAUSE:
				return;
			case SCHED_RESUME:
				return;
			case SCHED_RESCHEDULE:
				return;
			case SCHED_SHUTDOWN:
				handle_shutdown(s);
				break;
		}
		free(payload);
	}
}

static void handle_timerfd_wake(scheduler_t *s) {
	uint64_t expirations;
	read(s->timer_fd, &expirations, sizeof(expirations));

	uint64_t now = get_monotonic_ns();
	while (s->size > 0 && s->heap[0].next_run_ns <= now) { 
		// pop scheduled-entry from heap
		scheduler_entry_t entry = heap_pop(s); 
		task_t *task = entry.task;

		// in-case of cancelled/paused tasks 
		task_state_t state = atomic_load(&task->state);
		if (state == TASK_CANCELLED) 
			continue;
		
		// change task state to RUNNING & dispatch to worker pool queue for execution
		ready_entry_t *ready_task = malloc(sizeof(ready_entry_t));
		if (!ready_task) {
			// err handling 
			continue;
		}
		ready_task->task = task;
		ready_task->scheduled_run_ns = entry.next_run_ns;

		atomic_store(&task->state, TASK_RUNNING);
		atomic_store(&task->last_run_ns, now);
		
		async_queue_push(s->ready_queue, ready_task);
	}
	// rearm the min-heap task's timer for next execution 
	if (s->size > 0) {
		uint64_t nxt = s->heap[0].next_run_ns;
		struct itimerspec its = {0};
		its.it_value.tv_sec = nxt / 1000000000ULL;
		its.it_value.tv_nsec = nxt % 1000000000ULL;
		timerfd_settime(s->timer_fd, TFD_TIMER_ABSTIME, &its, NULL);
	}
	// next impl worker_entry_fn 
	// then completionfd below (signaled by worker)
}

static void handle_completionfd_wake(scheduler_t *s) {
	/*
	 * As of right now this doesnt need to do anything,
	 * timerfd wake triggers worker_entry_fn -> executes task -> pushes to output queue 
	 * if we move output queue push here it'd be unncessary syntatical overhead + awkward 
	 * to re-obtain the exact task from the heap, potentially even impact performance
	 * Might remove any 'completion' related functionality entirely
	*/
	
}

/*
 * 
*/
static void worker_entry_fn(ready_entry_t *entry, void *ctx_void) {
	if (!entry || !ctx_void) return;

	worker_ctx_t *ctx = (worker_ctx_t*)ctx_void;
	task_t *task = (task_t*)entry->task;

	// compute missed-run & additional time-related metadata
	uint64_t now_ns = get_monotonic_ns();
	uint64_t scheduled_ns = entry->scheduled_run_ns;
	atomic_store(&task->last_run_ns, scheduled_ns);

	uint64_t interval = task->interval_ns;
	if (interval > 0 && now_ns > scheduled_ns + interval) {
		uint64_t missed = (now_ns - scheduled_ns) / interval;
		atomic_store(&task->missed_runs, missed);
	}

	// execute task & compute additional task-runtime metadata
	task_state_t prev_state = atomic_exchange(&task->state, TASK_RUNNING);
	void *result = NULL;
	if (prev_state != TASK_CANCELLED) {
		uint64_t start_ns = get_monotonic_ns();
		result = task->task_fn(task->arg);
		uint64_t end_ns = get_monotonic_ns();
		atomic_store(&task->actual_run_ns, end_ns - start_ns);
		atomic_fetch_add(&task->run_count, 1);
	}
	
	// update task-state post execution
	if (prev_state != TASK_CANCELLED) 
		atomic_store(&task->state, TASK_SCHEDULED);
	else 
		atomic_store(&task->state, TASK_ZOMBIE);

	// push result to output queue for application to process 
	if (prev_state != TASK_CANCELLED && result != NULL) {
		task_result_t *task_result = malloc(sizeof(task_result_t));
		if (task_result) {
			task_result->task = task;
			task_result->result = result;
			async_queue_push(ctx->output_queue, task_result);
		}
	} 


	// update completion bitmap 
	int complete_id = task->completion_id;
	if (complete_id > 0 && complete_id < MAX_TASKS) {
		size_t idx = complete_id / 64;
		atomic_fetch_or(
			&ctx->completion_map[idx],
			1ULL << (complete_id % 64)
		);
		uint64_t x = 1;
		// wakes scheduler_run() fn's handle_completionfd_wake() 'else if' case 
		write(ctx->completion_eventfd, &x, sizeof(x));
	}

	free(entry);
} 

/* Core API */
scheduler_t *scheduler_init(
	size_t initial_workers, 
	size_t max_tasks, 
	async_queue_t *output_queue
) {
	if (max_tasks == 0 || max_tasks > MAX_TASKS || !output_queue)
		return NULL;

	scheduler_t *s = calloc(1, sizeof(scheduler_t));
	if (!s) return NULL;

	s->heap = calloc(max_tasks, sizeof(scheduler_entry_t));
	if (!s->heap) 
		goto fail_heap;

	s->size = 0;
	s->capacity = max_tasks;
	
	s->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
	if (s->epoll_fd < 0) 
		goto fail_epoll;

	s->timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
	if (s->timer_fd < 0) 
		goto fail_timerfd;
	struct epoll_event ev = {0};
	ev.events = EPOLLIN;
	ev.data.fd = s->timer_fd;
	if (epoll_ctl(s->epoll_fd, EPOLL_CTL_ADD, s->timer_fd, &ev) < 0) 
		goto fail_timer_epoll;

	s->ctrl_queue = async_queue_init(64);
	if (!s->ctrl_queue)
		goto fail_ctrl_queue;
	s->ctrl_eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (s->ctrl_eventfd < 0)
		goto fail_ctrl_eventfd;
	ev.events = EPOLLIN;
	ev.data.fd = s->ctrl_eventfd;
	if (epoll_ctl(s->epoll_fd, EPOLL_CTL_ADD, s->ctrl_eventfd, &ev) < 0)
		goto fail_ctrl_epoll;

	s->output_queue = output_queue;

	worker_ctx_t ctx = {
		.completion_map = s->completion_map,
		.completion_eventfd = s->completion_eventfd,
		.output_queue = s->output_queue
	};

	s->ready_queue = async_queue_init(200);
	if (!s->ready_queue)
		goto fail_ready_queue;
	
	s->pool = worker_pool_init(initial_workers, s->ready_queue, worker_entry_fn, ctx);
	if (!s->pool)
		goto fail_worker_pool;

	atomic_store(&s->shutdown, 0);

	return s;

fail_worker_pool:
	async_queue_free(s->ready_queue);
fail_ready_queue:
	close(s->completion_eventfd);
fail_completion_eventfd:
	epoll_ctl(s->epoll_fd, EPOLL_CTL_DEL, s->ctrl_eventfd, NULL);
fail_ctrl_epoll:
	close(s->ctrl_eventfd);
fail_ctrl_eventfd:
	async_queue_free(s->ctrl_queue);
fail_ctrl_queue:
	epoll_ctl(s->epoll_fd, EPOLL_CTL_DEL, s->timer_fd, NULL);
fail_timer_epoll:
	close(s->timer_fd);
fail_timerfd:
	close(s->epoll_fd);
fail_epoll:
	free(s->heap);
fail_heap:
	free(s);
	return NULL;
}

void scheduler_shutdown(scheduler_t *s) {
/* Signal shutdown via cntrl queue & drain the queue in case of pending requests */
	if (!s) return;

	scheduler_ctrl_payload *state_req = malloc(sizeof(scheduler_ctrl_payload));
	if (!state_req) return;

	state_req->op = SCHED_SHUTDOWN;
	state_req->task = NULL;
	state_req->new_interval_ns = 0;

	async_queue_push(s->ctrl_queue, state_req);
		
	uint64_t x = 1;
	// wakes epoll_wait in scheduler_run(), prompting call to handle_ctrl_request()
	// function's SCHED_SHUTDOWN switch-case to handle graceful shutdown of tasks
	write(s->ctrl_eventfd, &x, sizeof(x));
}

int scheduler_run(scheduler_t *s) {
	if (!s) return -1;

	while (!atomic_load(&s->shutdown)) {
		struct epoll_event evs[8];
		int nfds = epoll_wait(s->epoll_fd, evs, 8, -1); // block here 
		
		if (nfds < 0) {
			if (errno == EINTR) continue;
			return -1;
		}

		for (int i = 0; i < nfds; i++) {
			int fd = evs[i].data.fd;

			if (fd == s->ctrl_eventfd) {
				// some function wrote a uint64_t value; 1
				handle_ctrl_request(s);
			} 
			else if (fd == s->timer_fd) {
				// timerfd_settime()-fire triggered this fd 
				handle_timerfd_wake(s);
			}
			else if (fd == s->completion_eventfd) {
				// a worker thread wrote a uint64_t value; 1 
				handle_completionfd_wake(s); 
			}
		}
	}


	return 0;
}
