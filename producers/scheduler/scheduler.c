#include "scheduler.h"
#include "../../utils/heap.h"
#include <bits/time.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdatomic.h>
#include <string.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <sys/timerfd.h>
#include <sys/eventfd.h>

scheduler_t *scheduler_init(
	worker_entry_fn_t entry_fn,
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
	s->timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
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
	
	s->pool = worker_pool_init(initial_workers, s->ready_queue, entry_fn, ctx);
	if (!s->pool)
		goto fail_worker_pool;

	atomic_store(&s->shutdown, 0);

	return s;

fail_worker_pool:
	epoll_ctl(s->epoll_fd, EPOLL_CTL_DEL, s->completion_eventfd, NULL);
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


int main(void) {

}


