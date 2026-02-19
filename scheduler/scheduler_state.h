#ifndef SCHEDULER_STATEH
#define SCHEDULER_STATEH

#include "scheduler.h"

void sched_ctrl_shutdown(scheduler_t *s);
int sched_ctrl_add_task(scheduler_t *s, task_t *task);
int sched_ctrl_rm_task(scheduler_t *s, task_t *task);
int sched_ctrl_pause_task(scheduler_t *s, task_t *task);
int sched_ctrl_resume_task(scheduler_t *s, task_t *task);
int sched_ctrl_reschedule_task(scheduler_t *s, task_t *task);

#endif

