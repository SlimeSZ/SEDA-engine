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
#include <stdbool.h>
#include <stdio.h>

void sched_shutdown(scheduler_t *s) {
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

int sched_add_task(scheduler_t *s, task_t *task) {
	if (!s || !task)
		return -1;

	task_state_t curr_state = atomic_load(&task->state);
	// won't this be ... || state == task_cancelled? (for cancelled we dont call this fn or do we?)
	if (curr_state != TASK_REGISTERED && curr_state != TASK_CANCELLED)
		return -1;
		
	// init new scheduler task-entry
	scheduler_entry_t *sched_task = malloc(sizeof(scheduler_entry_t));
	if (!sched_task)
		return -1;
	uint64_t now = get_monotonic_ns();
	uint64_t next_run_ns = now + task->interval_ns;
	sched_task->task = task;
	sched_task->next_run_ns = next_run_ns;

	if (s->size >= s->capacity) {
		free(sched_task);
		return -1;
	}

	uint64_t old_min_deadline = (s->size > 0) ? s->heap[0].next_run_ns : UINT64_MAX;

	// push to heap (not thread safe for now but only this adds to heap, so fine for now)
	if (heap_push(s, sched_task) != 0) {
		free(sched_task);
		return -1;
	}

	atomic_store(&task->state, TASK_SCHEDULED);

	// rearm timer if new task is earliest
	uint64_t new_min_deadline = s->heap[0].next_run_ns;
	if (new_min_deadline < old_min_deadline) {
		struct itimerspec its = {0};
		its.it_value.tv_sec = new_min_deadline / 1000000000ULL;
		its.it_value.tv_nsec = new_min_deadline % 1000000000ULL;
		if (timerfd_settime(s->timer_fd, TFD_TIMER_ABSTIME, &its, NULL) < 0) 
			return -1;
	}

	free(sched_task);
	return 0;
}

int scheduler_rm_task(scheduler_t *s, task_t *task) {
	if (!s || !task) return -1;

	// task must NOT be running or either cancelled or zombie
	task_state_t curr_state = task->state;
	switch (curr_state) {
		case TASK_RUNNING:
			// can't pop while executing, worker will see state-change 
			// and set to zombie after task-execution
			atomic_store(&task->state, TASK_CANCELLED);
			return 0;
		case TASK_CANCELLED: case TASK_ZOMBIE:
			// already removed or in process of being removed 
			return 0;
		case TASK_REGISTERED:
			// not in heap, can mark cancelled 
			atomic_store(&task->state, TASK_CANCELLED);
			return 0;
		case TASK_SCHEDULED:
			// need to rm from heap
			break;
	}
	
	size_t task_idx = SIZE_MAX;
	for (size_t i = 0; i < s->size; i++) {
		if (s->heap[i].task == task) {
			task_idx = i; break;
		}
	}
	if (task_idx == SIZE_MAX) {
		atomic_store(&task->state, TASK_CANCELLED);
		return -1;
	}

	bool was_earliest_task = (task_idx == 0);

	s->size--;
	if (s->heap[task_idx].task != s->heap[s->size].task) {
		s->heap[task_idx] = s->heap[s->size];
		heapify_down(s, task_idx);
		heapify_up(s, task_idx);
	}
	atomic_store(&task->state, TASK_CANCELLED);

	if (was_earliest_task && s->size > 0) {
		uint64_t new_min_deadline = s->heap[0].next_run_ns;
		struct itimerspec its = {0};
		its.it_value.tv_sec = new_min_deadline / 1000000000ULL;
		its.it_value.tv_nsec = new_min_deadline % 1000000000ULL;
		timerfd_settime(s->timer_fd, TFD_TIMER_ABSTIME, &its, NULL);
	} else if (s->size == 0) {
		struct itimerspec its = {0};
		timerfd_settime(s->timer_fd, 0, &its, NULL);
	}

	return 0;
}




int scheduler_pause_task(scheduler_t *s, task_t *task);
int scheduler_resume_task(scheduler_t *s, task_t *task);
int scheduler_reschedule_task(scheduler_t *s, task_t *task);
void scheduler_reset(scheduler_t *s);
