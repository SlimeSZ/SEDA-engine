#ifndef SCHEDULER_H
#define SCHEDULER_H

#include <stdint.h> 
#include "../utils/async_queue.h"
#include "../utils/worker_pool.h"

typedef enum {
	SCHED_ADD = 0,
	SCHED_ADD_MANY,
	SCHED_REMOVE,
	SCHED_RESCHEDULE,
	SCHED_PAUSE,
	SCHED_RESUME
} scheduler_ctrl_op_t;

typedef enum {
	TASK_MISS_SKIP = 0, 			// discard any missed executions, no backlog, predictable rate 
	TASK_MISS_COALESCE,			// missed executions collapse into a single execution 
	TASK_MISS_CATCHUP			// replay N missed executions N times consecutively
} task_miss_policy_t;


typedef struct task_t task_t;
typedef struct {
	scheduler_ctrl_op_t op;
	union {
		struct {
			void *(*task_fn)(void*arg);
			void *arg;
			uint64_t interval_ns;
			task_miss_policy_t miss_policy;
		} add;
		struct {
				
		} add_many;

		struct {
		} remove;

	};
} scheduler_ctrl_payload_t;

typedef enum {
	TASK_REGISTERED = 0,
	TASK_SCHEDULED,
	TASK_RUNNING,
	TASK_CANCELLED,
	TASK_ZOMBIE
} task_state_t;

struct task_t {
	void *(*task_fn)(void*arg);
	void *result;
	void *arg;
	uint64_t interval_ns;

	int completion_id;

	task_miss_policy_t miss_policy;
	_Atomic task_state_t state;

	_Atomic uint64_t scheduled_ns;
	_Atomic uint64_t last_run_ns;
	_Atomic uint64_t actual_run_ns;
	_Atomic uint64_t run_count;
	_Atomic uint64_t missed_runs;
};

/* Wrapper around heap task obj */
typedef struct {
	task_t *task; 
	uint64_t next_run_ns;
} scheduler_entry_t; 

typedef struct scheduler_t {
	scheduler_entry_t *heap;
	size_t size;
	size_t capacity;

	int epoll_fd;

	async_queue_t *task_ready_queue;
	int timer_fd;

	async_queue_t *sched_ctrl_queue;
	int sched_ctrl_fd;

	_Atomic uint64_t task_completion_map[64]; // FIX
	int task_completion_fd;

	async_queue_t *task_output_queue;

	worker_pool_t *worker_pool;

	_Atomic int shutdown;
} scheduler_t;

uint64_t get_monotonic_ns(void);

/* API 
scheduler_t *scheduler_init(size_t initial_workers,
		size_t max_tasks, async_queue_t *output_queue);
void scheduler_shutdown(scheduler_t *s);
int scheduler_run(scheduler_t *s);
void sched_shutdown(scheduler_t *s);
int sched_add_task(scheduler_t *s, task_t *task);
int sched_rm_task(scheduler_t *s, task_t *task);
int sched_pause_task(scheduler_t *s, task_t *task);
int sched_resume_task(scheduler_t *s, task_t *task);
int sched_resched_task(scheduler_t *s, task_t *task);
void scheduler_reset(scheduler_t *s);
uint64_t scheduler_next_run(const scheduler_t *s);
void scheduler_wake(scheduler_t *s);
void scheduler_view_completed(scheduler_t *s);
uint64_t get_monotonic_ns(void);
*/
#endif

