#ifndef SCHEDULER_STATEH
#define SCHEDULER_STATEH

#include "scheduler.h"

void sched_ctrl_shutdown(scheduler_t *s);
int sched_ctrl_add_task(
	scheduler_t *s, 
	void *(*task_fn)(void*arg), 
	void *arg,
	uint64_t interval_ns,
	task_miss_policy_t miss_policy
); 
int sched_ctrl_rm_task(scheduler_t *s, task_t *task);
int sched_ctrl_pause_task(scheduler_t *s, task_t *task);
int sched_ctrl_resume_task(scheduler_t *s, task_t *task);
int sched_ctrl_reschedule_task(scheduler_t *s, task_t *task);

#endif

