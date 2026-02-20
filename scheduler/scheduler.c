#include "scheduler.h"
#include "scheduler_state.h"
#include "../utils/heap.h"
#include "../utils/async_queue.h"
#include "../utils/worker_pool.h"
#include <bits/time.h>
#include <bits/types/struct_itimerspec.h>
#include <pthread.h>
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

inline __attribute__((always_inline, hot))
uint64_t get_monotonic_ns(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static void sched_state_change(scheduler_t *s) {
	uint64_t x;
	read(s->sched_ctrl_fd, &x, sizeof(x));

	scheduler_ctrl_payload_t *payload;
	while ((payload = async_queue_try_pop(s->sched_ctrl_queue)) != NULL) {
		switch (payload->op) {
			case SCHED_ADD:
				sched_ctrl_add_task(
					s,
					payload->add.task_fn,
					payload->add.arg,
					payload->add.interval_ns,
					payload->add.miss_policy
				);
				break;
			case SCHED_ADD_MANY:
				//for (size_t i = 0; i < payload->add_many.count; i++) 
				//	sched_ctrl_add_task(s, payload->add_many.tasks[i]);
				//free(payload->add_many.tasks);
				break;
			case SCHED_REMOVE:
				break;
			case SCHED_PAUSE:
				break;
			case SCHED_RESUME:
				break;
			case SCHED_RESCHEDULE:
				break;
		}
		free(payload);
	}
}

/*
 * Triggered by a scheduler timer_fd wake
 * continously pops heap entries (tasks to be executed)
 * ensures no spurious task cancellation
 * sets task state to RUNNING alongside last_run_ns metadata 
 * pushes to ready queue for worker_routine to wake & execute
 * rearms min heap timer (next task) for execution
*/
static void task_timerfd_wake(scheduler_t *s) {
	uint64_t fires;
	read(s->timer_fd, &fires, sizeof(fires));
	
	uint64_t now = get_monotonic_ns();

	while (s->size > 0 && s->heap[0].next_run_ns <= now) {
		scheduler_entry_t heap_entry = heap_pop(s);
		task_t *task = heap_entry.task;
		
		task_state_t task_state = atomic_load(&task->state);
		if (task_state == TASK_CANCELLED || task_state == TASK_ZOMBIE)
			continue;
		printf("[timerfd_wake()] task state: %c\n", task_state);

		atomic_store(&task->state, TASK_RUNNING);
		atomic_store(&task->scheduled_ns, get_monotonic_ns());

		async_queue_push(s->task_ready_queue, task);
	}

	if (s->size > 0) {
		uint64_t nxt = s->heap[0].next_run_ns;
		struct itimerspec its = {0};
		its.it_value.tv_sec = nxt / 1000000000ULL;
		its.it_value.tv_nsec = nxt % 1000000000ULL;
		timerfd_settime(s->timer_fd, TFD_TIMER_ABSTIME, &its, NULL);
	}
}

/*
 * Called by worker thread routine fn once it pops from ready queue, all workers 
 * call this fn, which then executes the actual user-specified task, storing result 
 * in output queue or discarding if not needed, as stated in C doc of worker_thread_routine
 * fn however, I may, in the future, add more robust parameters to dictate how task 
 * result is handled via worker_ctx_t
*/
void *entry_fn(void *void_task, void *void_ctx) {
	worker_ctx_t *ctx = (worker_ctx_t*)void_ctx;
	task_t *task = (task_t*)void_task;

	uint64_t now_ns = get_monotonic_ns();
	uint64_t scheduled_ns = atomic_load(&task->scheduled_ns); 
	atomic_store(&task->last_run_ns, scheduled_ns);
	uint64_t interval_ns = task->interval_ns;

	if (interval_ns > 0 && now_ns > scheduled_ns + interval_ns) {
		uint64_t missed = (now_ns - scheduled_ns) / interval_ns;
		atomic_store(&task->missed_runs, missed);
		// shouldnt we return here??	
	}

	task_state_t prev_state = atomic_exchange(&task->state, TASK_RUNNING);
	if (prev_state != TASK_CANCELLED) {
		uint64_t start_ns = get_monotonic_ns();
		task->result = task->task_fn(task->arg);
		uint64_t end_ns = get_monotonic_ns();
		atomic_store(&task->actual_run_ns, end_ns - start_ns);
		atomic_fetch_add(&task->run_count, 1);
		atomic_store(&task->state, TASK_SCHEDULED);

		printf("[entry_fn()] task executed - res: %p\n",
			task->result);
		
		scheduler_entry_t entry = {
			.task = task,
			.next_run_ns = scheduled_ns + interval_ns
		};
		heap_push(ctx->sched, &entry);
		uint64_t nxt = ctx->sched->heap[0].next_run_ns;
		struct itimerspec its = {0};
		its.it_value.tv_sec  = nxt / 1000000000ULL;
		its.it_value.tv_nsec = nxt % 1000000000ULL;
		timerfd_settime(ctx->sched->timer_fd, TFD_TIMER_ABSTIME, &its, NULL);
	} else {
		atomic_store(&task->state, TASK_ZOMBIE);
		printf("[entry_fn()] task was cancelled - exiting without execution\n");
		return NULL;
	}

	if (task->result != NULL) {
		async_queue_push(ctx->output_queue, task->result);
		int completion_id = task->completion_id;
		if (completion_id > 0) {
			size_t idx = completion_id / 64;
			atomic_fetch_or(&ctx->completion_map[idx], 
				1ULL << (completion_id % 64));
			uint64_t x = 1;
			// can optionally write to wake completion fd, but this has 
			// no effect as of right now, completion-bitmap update alone 
			// suffices
		}
	}

	return NULL; 
}

scheduler_t *scheduler_init(
	size_t workers,
	size_t num_tasks,
	async_queue_t *output_queue
) {
	if (workers > MAX_WORKERS_PER_SCHEDULER || !output_queue)
		return NULL;

	scheduler_t *s = calloc(1, sizeof(scheduler_t));
	if (!s) return NULL;

	s->heap = calloc(num_tasks, sizeof(scheduler_entry_t));
	if (!s->heap)
		goto fail_heap;
	s->size = 0;
	s->capacity = 100000;
	
	/* core epoll fd for blocking */
	if ((s->epoll_fd = epoll_create1(EPOLL_CLOEXEC)) < 0)
		goto fail_epoll;

	/* timer_fd for task-ready signal */
	if ((s->timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)) < 0)
		goto fail_timerfd;
	struct epoll_event ev = {0};
	ev.events = EPOLLIN;
	ev.data.fd = s->timer_fd;
	if (epoll_ctl(s->epoll_fd, EPOLL_CTL_ADD, s->timer_fd, &ev) < 0) 
		goto fail_timer_epoll;

	/* event_fd for sched ctrl state change request signal */
	s->sched_ctrl_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC); 
	if (s->sched_ctrl_fd < 0)
		goto fail_ctrl_eventfd;
	ev.events = EPOLLIN;
	ev.data.fd = s->sched_ctrl_fd;
	if (epoll_ctl(s->epoll_fd, EPOLL_CTL_ADD, s->sched_ctrl_fd, &ev) < 0)
		goto fail_ctrl_epoll;

	/* epoll_fd for task completed signal */
	if ((s->task_completion_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) < 0)
		goto fail_task_completion_fd;

	/* task-ready, task-result-output, & sched-ctrl-state req queue inits */
	s->task_ready_queue = async_queue_init(num_tasks);
	if (!s->task_ready_queue) 
		goto fail_ready_queue;
	s->task_output_queue = output_queue;
	s->sched_ctrl_queue = async_queue_init(10);
	if (!s->sched_ctrl_queue)
		goto fail_ctrl_queue;

	/* worker context for pool */
	worker_ctx_t worker_ctx = {
		.completion_map = s->task_completion_map,
		.completion_eventfd = s->task_completion_fd,
		.output_queue = s->task_output_queue,
		.ready_queue = s->task_ready_queue,
		.entry_fn = &entry_fn,
		.sched = s
	};
	s->worker_pool = worker_pool_init(workers, worker_ctx);
	if (!s->worker_pool) 
		goto fail_worker_pool;

	atomic_store(&s->shutdown, 0);
	return s;

fail_worker_pool:
	async_queue_free(s->sched_ctrl_queue);
fail_ctrl_queue:
	async_queue_free(s->task_ready_queue);
fail_ready_queue:
	close(s->task_completion_fd);
fail_task_completion_fd:
	epoll_ctl(s->epoll_fd, EPOLL_CTL_DEL, s->sched_ctrl_fd, NULL);
fail_ctrl_epoll:
	close(s->sched_ctrl_fd);
fail_ctrl_eventfd:
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


/*
 * API for scheduler state changes, write to scheduler's state-control queue, prompting
 * 1st if stmnt in scheduler_run() to wake, which then calls sched_state_change(),
 * popping state-change request(s) from the queue and executing accordingly
 * NOTE however; that for scheduler_shutdown, the main scheduler_run() fn exits the while 
 * loop, calling sched_shutdown() before exiting the program, hence we do not push 
 * an explicit SCHED_SHUTDOWN enum to the queue, and no such enum exists
*/

void scheduler_shutdown(scheduler_t *s) {
	atomic_store(&s->shutdown, 1); // negate while condition of scheduler_run()
	uint64_t x = 1;
	// wake from epoll_wait() to see negation & shutdown before exiting
	write(s->sched_ctrl_fd, &x, sizeof(x));  
}

int scheduler_run(scheduler_t *s) {
	if (!s) 
		return -1;

	int total_completed = 0;

	while (!atomic_load(&s->shutdown)) {
		struct epoll_event evs[8];
		int nfds = epoll_wait(s->epoll_fd, evs, 8, -1);

		if (nfds < 0) {
			if (errno == EINTR) 
				continue;
			return -1;
		}

		for (int i = 0; i < nfds; i++) {
			int fd = evs[i].data.fd;

			if (fd == s->sched_ctrl_fd) 
				sched_state_change(s);

			else if (fd == s->timer_fd) 
				task_timerfd_wake(s);

			else if (fd == s->task_completion_fd) {
				total_completed++;
				printf("Completed a new task! Total Completed: %d\n",
					total_completed);
			}	
		}
	}

	sched_ctrl_shutdown(s);
	return 0;
}

/* ----------------------- test ------------------------------ */

#include <unistd.h>

static int param1 = 1,  param2 = 2,  param3 = 3,  param4 = 4,  param5 = 5;
static int param6 = 6,  param7 = 7,  param8 = 8,  param9 = 9,  param10 = 10;

void *foo1(void *arg)  { printf("[foo1]  %d\n", *(int*)arg); return arg; }
void *foo2(void *arg)  { printf("[foo2]  %d\n", *(int*)arg); return arg; }
void *foo3(void *arg)  { printf("[foo3]  %d\n", *(int*)arg); return arg; }
void *foo4(void *arg)  { printf("[foo4]  %d\n", *(int*)arg); return arg; }
void *foo5(void *arg)  { printf("[foo5]  %d\n", *(int*)arg); return arg; }
void *foo6(void *arg)  { usleep(800000); printf("[foo6-slow]  %d\n", *(int*)arg); return arg; }
void *foo7(void *arg)  { usleep(800000); printf("[foo7-slow]  %d\n", *(int*)arg); return arg; }
void *foo8(void *arg)  { usleep(800000); printf("[foo8-slow]  %d\n", *(int*)arg); return arg; }

void *foo9(void *arg)  { 
    volatile long x = 0; 
    for (long i = 0; i < 50000000L; i++) x += i; 
    printf("[foo9-spin]  %d\n", *(int*)arg); 
    return arg; 
}
void *foo10(void *arg) { 
    volatile long x = 0; 
    for (long i = 0; i < 50000000L; i++) x += i; 
    printf("[foo10-spin] %d\n", *(int*)arg); 
    return arg; 
}

void *output_routine(void *arg) {
    async_queue_t *out_q = (async_queue_t *)arg;
    void *res;
    while ((res = async_queue_pop(out_q)) != NULL)
        printf("Result: %d\n", *(int*)res);
    return NULL;
}

int main(void) {
    pthread_t out_thread;
    async_queue_t *out_q = async_queue_init(100);
    if (!out_q) return -1;
    pthread_create(&out_thread, NULL, output_routine, out_q);

    scheduler_t *sched = scheduler_init(20, 10, out_q); // 20 workers, 10 tasks
    if (!sched) return -1;

    uint64_t ms = 1000000ULL;

    sched_ctrl_add_task(sched, foo1,  &param1,  500  * ms, TASK_MISS_SKIP);
    sched_ctrl_add_task(sched, foo2,  &param2,  500  * ms, TASK_MISS_SKIP);
    sched_ctrl_add_task(sched, foo3,  &param3,  750  * ms, TASK_MISS_SKIP);
    sched_ctrl_add_task(sched, foo4,  &param4,  750  * ms, TASK_MISS_SKIP);
    sched_ctrl_add_task(sched, foo5,  &param5,  1000 * ms, TASK_MISS_SKIP);
    sched_ctrl_add_task(sched, foo6,  &param6,  1000 * ms, TASK_MISS_SKIP); // slow
    sched_ctrl_add_task(sched, foo7,  &param7,  1000 * ms, TASK_MISS_SKIP); // slow
    sched_ctrl_add_task(sched, foo8,  &param8,  1500 * ms, TASK_MISS_SKIP); // slow
    sched_ctrl_add_task(sched, foo9,  &param9,  1500 * ms, TASK_MISS_SKIP); // spin
    sched_ctrl_add_task(sched, foo10, &param10, 2000 * ms, TASK_MISS_SKIP); // spin

    scheduler_run(sched);

    async_queue_shutdown(out_q);
    pthread_join(out_thread, NULL);
    async_queue_free(out_q);
    return 0;
}






















