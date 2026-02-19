#include "scheduler_state.h"
#include "scheduler.h"
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
		printf("[sched_shutdown()]- Task state while being heap popped: %c\n",
			task->state);
		if (task) 
			atomic_store(&task->state, TASK_CANCELLED);
	}

	// 3) shutdown rdy queue - all threads blocking anywhere on queue exit
	//    via broadcast to both conditions + _Atomic shutdown flag 
	async_queue_shutdown(s->task_ready_queue);

	// 4) drain rdy queue - also ensures all tasks are indeed CANCELLED 
	task_t *pending_task;
	while ((pending_task = async_queue_try_pop(s->task_ready_queue)) != NULL) {
		if (pending_task->state != TASK_CANCELLED)
			atomic_store(&pending_task->state, TASK_CANCELLED);
	}

	// 5) 
	async_queue_free(s->task_ready_queue);

	// 6) shutdown worker pool - joins on all mid-execution
	worker_pool_shutdown(s->worker_pool);

	// 7) 
	scheduler_ctrl_payload_t *pending_sched_req;
	while ((pending_sched_req = async_queue_try_pop(s->sched_ctrl_queue)) != NULL) 
		free(pending_sched_req);
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
	
	free(s);
}

int sched_ctrl_add_task(scheduler_t *s, task_t *task) {




	return 0;
}












