#ifndef SCHEDULER_H
#define SCHEDULER_H

#include <pthread.h>
#include <stdint.h>
#include <stdatomic.h>
#include "../../utils/async_queue.h"
#include "../../utils/worker_pool.h"

#define MAX_TASKS 448

typedef struct task_t task_t;
/* Control Plane Queue (allow run-time scheduler heap mutations) without race conditions */
/* Action state requests for scheduler thread */
typedef enum {
	SCHED_ADD = 0,				// will create new heap entry and scheduler next_run_ns
	SCHED_REMOVE,				// skip if in heap then pop, or await completion before popping
	SCHED_RESCHEDULE,			// interval change, reorder heap as necessary
	SCHED_PAUSE,				// keep task registered but temporarily stop execution, ignored until SCHED_RESUME 
	SCHED_RESUME,				// re-enable previously SCHED_PAUSE task 
	SCHED_SHUTDOWN
} scheduler_ctrl_operation_t;
/*
 * SCHED_ADD -> TASK_REGISTERED -> TASK_SCHEDULED
 * SCHED_REMOVE -> TASK_CANCELLED
 * SCHED_RESCHEDULE -> ** updates next_run_ns **
 * SCHED_PAUSE/SCHED_RESUME -> ** state may remaing TASK_SCHEDULED, internal flag indicates state **
 * SCHED_SHUTDOWN -> TASK_CANCELLED -> TASK_ZOMBIE
*/
typedef struct {
	scheduler_ctrl_operation_t op;
	task_t *task;
	uint64_t new_interval_ns;
} scheduler_ctrl_payload;

typedef enum {
	TASK_REGISTERED = 0, 			// exists but not scheduled in heap 
	TASK_SCHEDULED,				// heap entry waiting next execution at next_run_ns
	TASK_RUNNING,				// actively executing, modifications must wait for state finish
	TASK_CANCELLED, 	 		// may be in heap or running, in which case must pop or wait for execution 
	TASK_ZOMBIE 				// finished and removed from scheduling, awaiting cleanup and memory reclamation
} task_state_t;

typedef enum {
	TASK_MISS_SKIP = 0, 			// discard any missed executions, no backlog, predictable rate 
	TASK_MISS_COALESCE,			// missed executions collapse into a single execution 
	TASK_MISS_CATCHUP			// replay N missed executions N times consecutively
} task_miss_policy_t;

typedef struct {
	task_t *task;
	void *result;
} task_result_t;

struct task_t {
	void *(*task_fn)(void *arg);	
	void *arg;	
	uint64_t interval_ns;

	int completion_id;			// index into global completion bitmap 
	
	task_miss_policy_t miss_policy;
	_Atomic task_state_t state;

	_Atomic uint64_t last_run_ns;
	_Atomic uint64_t actual_run_ns;
	_Atomic uint64_t run_count;
	_Atomic uint64_t missed_runs;
};


/* (what lives inside the heap) */
typedef struct {
	task_t *task; 
	uint64_t next_run_ns;
} scheduler_entry_t; 

typedef struct {
	scheduler_entry_t *heap;		// min-heap for earliest task guarantee without extensive linear scan
	size_t size;
	size_t capacity;

	int epoll_fd; 				// block until epoll_wait (triggered by ctrl_eventfd or timer_fd) 

	async_queue_t *ctrl_queue;		// queue and epoll signal for atomic task-state modification requests
	int ctrl_eventfd; 

	async_queue_t *ready_queue;		// queue to dispatch scheduled task to one of N worker threads for execution 
	
	_Atomic uint64_t completion_map[MAX_TASKS/64];
	int completion_eventfd;			// epoll signals the scheduler to reschedule a completed task 

	async_queue_t *output_queue;	 	// completed tasks pushed here for application to process 

	int timer_fd; 			 	// epoll signals the next scheduled task's execution time 
	
	worker_pool_t *pool;			// pool of N worker threads, each used to execute M tasks

	_Atomic int shutdown;
} scheduler_t;

/*
          ┌──────────────────────┐
          │  Scheduler Thread    │
          │  (reactor, heap,     │
          │   dispatch, control) │
          └─────────┬────────────┘
                    │
          ┌─────────┴─────────┐
          │   ready_queue     │
          └─────────┬─────────┘
   ┌───────────┐ ┌───────────┐ ... ┌───────────┐
   │ Worker 1  │ │ Worker 2  │     │ Worker N  │
   │ Executes  │ │ Executes  │     │ Executes  │
   │ task_fn   │ │ task_fn   │     │ task_fn   │
   │ Sets bit  │ │ Sets bit  │     │ Sets bit  │
   └────┬──────┘ └────┬──────┘     └────┬──────┘
        │             │                │
        └───────┬─────┴────────────────┘
                ▼
          completion_map (bitmap)
                │
        Scheduler wakes on eventfd
                │
        Re-inserts recurring tasks
                │
           output_queue → application
*/

/* API */
scheduler_t *scheduler_init(size_t initial_workers,
		size_t max_tasks, async_queue_t *output_queue);
void scheduler_shutdown(scheduler_t *s);
int scheduler_run(scheduler_t *s);
/* scheduler state-change control-plane utils */
void sched_shutdown(scheduler_t *s);
int sched_add_task(scheduler_t *s, task_t *task);
int sched_rm_task(scheduler_t *s, task_t *task);
int sched_pause_task(scheduler_t *s, task_t *task);
int sched_resume_task(scheduler_t *s, task_t *task);
int sched_resched_task(scheduler_t *s, task_t *task);
void scheduler_reset(scheduler_t *s);
/* insights + debugging */
uint64_t scheduler_next_run(const scheduler_t *s);
void scheduler_wake(scheduler_t *s);
void scheduler_view_completed(scheduler_t *s);
/* utilities */
uint64_t get_monotonic_ns(void);

#endif

/*	TODO:
 * make use of completion metadata/queue or cut it completely
 * 
*/
