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
		// printf("[sched_shutdown()]- Task state while being heap popped: %d\n",
			// atomic_load(&task->state));
		if (task) {
			atomic_store(&task->state, TASK_CANCELLED);
			free(task);
		}
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
		if (atomic_load(&pending_task->state) != TASK_CANCELLED)
			atomic_store(&pending_task->state, TASK_CANCELLED);
		free(pending_task);
	}
	printf("[sched_shutdown()] rdy q drained - size: %d\n", 
		atomic_load(&s->task_ready_queue->size));

	// 5) 
	async_queue_free(s->task_ready_queue);

	// 6) shutdown worker pool - joins on all mid-execution
	worker_pool_shutdown(s->worker_pool);

	// 7) 
	scheduler_ctrl_payload_t *pending_sched_req;
	while ((pending_sched_req = async_queue_try_pop(s->sched_ctrl_queue)) != NULL) { 
		if (pending_sched_req->op == SCHED_ADD_MANY)
			free(pending_sched_req->add_many.tasks);
		free(pending_sched_req);
	}
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
	pthread_mutex_destroy(&s->task_id_lock);

	printf("[sched_shutdown()] scheduler completely shutdown\n");
	
	free(s);
}

int sched_ctrl_add_task(scheduler_t *s, user_task_t user_task) {
	if (s->size >= s->capacity)
		return -1;

	if (
		!user_task.task_fn || 
		user_task.interval_ns == 0 || 
		user_task.miss_policy > TASK_MISS_CATCHUP
	) 
		return -1;

	// init task_t 
	task_t *task = calloc(1, sizeof(task_t));
	if (!task) 
		return -1;

	task->id = user_task.id;
	s->task_registry[task->id] = task;
	task->task_fn = user_task.task_fn;
	task->arg = user_task.arg;
	task->interval_ns = user_task.interval_ns;
	task->miss_policy = user_task.miss_policy;

	uint64_t old_min_deadline = (s->size > 0) ? s->heap[0].next_run_ns : UINT64_MAX;
	
	// push to heap 
	scheduler_entry_t heap_entry = {
		.task = task,
		.next_run_ns = get_monotonic_ns() + user_task.interval_ns
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

int sched_ctrl_add_many_tasks(
	scheduler_t *s,
	user_task_t *user_tasks,
	size_t count
) {
	if (s->size >= s->capacity || count == 0)
		return -1;
	
	int pushed = 0;

	for (size_t i = 0; i < count; i++) 
		if (sched_ctrl_add_task(s, user_tasks[i]) == 0)
			pushed++;

	return pushed;
}
/*
int sched_ctrl_rm_task(scheduler_t *s, int task_id) {
    printf("[rm_task] called for id=%d, heap size=%zu\n", task_id, s->size);
    for (size_t i = 0; i < s->size; i++) {
        task_t *task = s->heap[i].task;
        if (task->id != task_id) continue;

        printf("[rm_task] found id=%d at heap[%zu], state=%d\n", task_id, i, atomic_load(&task->state));
        task_state_t old_state = atomic_exchange(&task->state, TASK_CANCELLED);
        if (old_state == TASK_RUNNING) {
            printf("[rm_task] id=%d was RUNNING — leaving to entry_fn\n", task_id);
            return 0;
        }
        heap_remove(s, task_id);
        pthread_mutex_lock(&s->task_id_lock);
        s->task_id_bitmap[task_id/64] &= ~(1ULL << (task_id % 64));
        pthread_mutex_unlock(&s->task_id_lock);
        free(task);
        printf("[rm_task] id=%d removed successfully\n", task_id);
        return 0;
    }
    printf("[rm_task] id=%d NOT FOUND in heap\n", task_id);
    return -1;
}
*/
int sched_ctrl_rm_task(scheduler_t *s, int task_id) {
	// task can be in one of 3 states: heap, rdy q, executing (popped by worker)
	printf("[rm_task] called for id=%d, heap size=%zu\n", task_id, s->size);

	// case 1: heap
	for (size_t i = 0; i < s->size; i++) {
		task_t *task = s->heap[i].task;
		if (task->id != task_id)
			continue;

		task_state_t old_state = atomic_exchange(&task->state, TASK_CANCELLED);
		if (old_state == TASK_RUNNING) { 
			// alrdy popped from heap via timerfd_wake, 
			// mid-execution entry_fn will detect TASK_CANCELLED, set ZOMBIE,
			// & skip reschedule
			printf("[rm_task] id=%d was RUNNING — leaving to entry_fn\n", task_id);
			return 0;
		}

		heap_remove(s, task_id);
		pthread_mutex_lock(&s->task_id_lock);
		s->task_id_bitmap[task_id/64] &= ~(1ULL << (task_id % 64));
		pthread_mutex_unlock(&s->task_id_lock);
		free(task);
		s->task_registry[task_id] = NULL;
		printf("[rm_task] id=%d removed successfully\n", task_id);
		return 0;
	}

	printf("[rm_task] id=%d not in heap — may be in ready queue, marking via task_ptr scan\n", task_id);

	// case 2: not in heap - may be in ready queue 
	if (task_id < MAX_TASKS_PER_SCHEDULER && s->task_registry[task_id] != NULL) {
		task_t *task = s->task_registry[task_id];
		task_state_t old_state = atomic_exchange(&task->state, TASK_CANCELLED);
		printf("[rm_task] id=%d found in registry, old state=%d — marked CANCELLED\n", task_id, old_state);
		return 0;
	}

	printf("[rm_task] id=%d NOT FOUND anywhere\n", task_id);
	return 0;
}














