#include "scheduler_state.h"
#include "../utils/heap.h"
#include "scheduler.h"
#include <stdint.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <unistd.h>

/*
 * 1) disarm timerfd - no tasks can wakeup except those mid-execution
 * 2) mark tasks as TASK_CANCELLED via heap - chance of tasks mid-execution to stop 
 * 3) ready queue shutdown - broadcasts to workers in thread_routine, waking them,
 *    breaking from loop 
 * 4) drain ready queue prior to freeing it - uncontested drain, workers have exited
 * 5) free ready queue - now safe 
 * 6) shutdown worker pool - pthread_join() on all mid-execution if any
 * 7) drain & free cntrl queue - no need to shutdown (broadcast), no threads waiting on it 
 * 8) free heap & close all fds before freeing scheduler
*/
void sched_ctrl_shutdown(scheduler_t *s) {
	// 1) disable timerfd 
	struct itimerspec its = {0};	
	timerfd_settime(s->timer_fd, 0, &its, NULL);

	// 2) mark all heap tasks CANCELLED 
	for (size_t i = 0; i < s->size; i++) {
		task_t *task = s->heap[i].task;
		// Ensure tasks are mostly SCHEDULED, else adjust code accordingly
		printf("[sched_shutdown()]- Task state while being heap popped: %d\n",
			atomic_load(&task->state));
		if (task) 
			atomic_store(&task->state, TASK_CANCELLED);
	}

	// 3) shutdown rdy queue - all threads blocking anywhere on queue exit
	//    via broadcast to both conditions + _Atomic shutdown flag 
	async_queue_shutdown(s->task_ready_queue);

	// 4) drain rdy queue - also ensures all tasks are indeed CANCELLED 
	task_t *pending_task;
	while ((pending_task = async_queue_try_pop(s->task_ready_queue)) != NULL) {
		printf(
			"[sched_shutdown()] drained pending task from rdy q - task state: %d\n",
			atomic_load(&pending_task->state)
		);
		if (pending_task->state != TASK_CANCELLED)
			atomic_store(&pending_task->state, TASK_CANCELLED);
	}
	printf("[sched_shutdown()] rdy q drained - size: %d\n", 
		atomic_load(&s->task_ready_queue->size));

	// 5) 
	async_queue_free(s->task_ready_queue);

	// 6) shutdown worker pool - joins on all mid-execution
	worker_pool_shutdown(s->worker_pool);

	// 7) 
	scheduler_ctrl_payload_t *pending_sched_req;
	while ((pending_sched_req = async_queue_try_pop(s->sched_ctrl_queue)) != NULL) 
		free(pending_sched_req);
	printf("[sched_shutdown()] ctrl q drained - size: %d\n",
		atomic_load(&s->sched_ctrl_queue->size));
	async_queue_free(s->sched_ctrl_queue);

	// 8)
	free(s->heap);
	epoll_ctl(s->epoll_fd, EPOLL_CTL_DEL, s->timer_fd, NULL);
	epoll_ctl(s->epoll_fd, EPOLL_CTL_DEL, s->sched_ctrl_fd, NULL);
	epoll_ctl(s->epoll_fd, EPOLL_CTL_DEL, s->task_completion_fd, NULL);
	close(s->timer_fd);
	close(s->sched_ctrl_fd);
	close(s->task_completion_fd);
	close(s->epoll_fd);

	printf("[sched_shutdown()] scheduler completely shutdown\n");
	
	free(s);
}

int sched_ctrl_add_task(
	scheduler_t *s, 
	void *(*task_fn)(void*arg), 
	void *arg,
	uint64_t interval_ns,
	task_miss_policy_t miss_policy
) {
	if (s->size >= s->capacity)
		return -1;

	if (
		!task_fn || 
		interval_ns == 0 || 
		miss_policy > TASK_MISS_CATCHUP
	) 
		return -1;
	
	// init task_t 
	task_t *task = calloc(1, sizeof(task_t));
	if (!task) 
		return -1;

	task->task_fn = task_fn;
	task->arg = arg;
	task->interval_ns = interval_ns;
	task->miss_policy = miss_policy;

	uint64_t old_min_deadline = (s->size > 0) ? s->heap[0].next_run_ns : UINT64_MAX;
	
	// push to heap 
	scheduler_entry_t heap_entry = {
		.task = task,
		.next_run_ns = get_monotonic_ns() + interval_ns
	};
	if (heap_push(s, &heap_entry) != 0) {
		free(task);
		return -1;
	}
	atomic_store(&task->state, TASK_SCHEDULED);

	uint64_t new_min_deadline = s->heap[0].next_run_ns;
	if (new_min_deadline < old_min_deadline) {
		struct itimerspec its = {0};
		its.it_value.tv_sec = new_min_deadline / 1000000000ULL;
		its.it_value.tv_nsec = new_min_deadline % 1000000000ULL;
		timerfd_settime(s->timer_fd, TFD_TIMER_ABSTIME, &its, NULL);
	}

	return 0;
}
/*
int sched_ctrl_add_task(
	scheduler_t *s, 
	void *(*task_fn)(void*arg), 
	void *arg,
	uint64_t interval_ns,
	task_miss_policy_t miss_policy
) {
	printf("[add_task] entered\n");
	if (s->size >= s->capacity) return -1;
	if (!task_fn || interval_ns == 0 || miss_policy > TASK_MISS_CATCHUP) return -1;
	
	printf("[add_task] allocating task\n");
	task_t *task = calloc(1, sizeof(task_t));
	if (!task) return -1;

	task->task_fn = task_fn;
	task->arg = arg;
	task->interval_ns = interval_ns;
	task->miss_policy = miss_policy;

	printf("[add_task] s=%p, s->heap=%p, s->size=%zu, s->capacity=%zu\n",
		(void*)s, (void*)s->heap, s->size, s->capacity);

	uint64_t old_min_deadline = (s->size > 0) ? s->heap[0].next_run_ns : UINT64_MAX;
	
	scheduler_entry_t heap_entry = {
		.task = task,
		.next_run_ns = get_monotonic_ns() + interval_ns
	};

	printf("[add_task] pushing to heap\n");
	if (heap_push(s, &heap_entry) != 0) {
		free(task);
		return -1;
	}

	printf("[add_task] heap pushed, size now: %zu\n", s->size);
	atomic_store(&task->state, TASK_SCHEDULED);

	uint64_t new_min_deadline = s->heap[0].next_run_ns;
	if (new_min_deadline < old_min_deadline) {
		struct itimerspec its = {0};
		its.it_value.tv_sec = new_min_deadline / 1000000000ULL;
		its.it_value.tv_nsec = new_min_deadline % 1000000000ULL;
		printf("[add_task] arming timerfd\n");
		timerfd_settime(s->timer_fd, TFD_TIMER_ABSTIME, &its, NULL);
	}

	printf("[add_task] done - interval_ns: %lu\n", task->interval_ns);
	return 0;
}
*/










