package com.scheduler;

import java.util.Objects;

public final class UserTask {

    @FunctionalInterface
    public interface TaskFn {
	    Object run(Object arg);
    }

    private final TaskFn taskFn;
    private final Object arg;
    private final long intervalNs;
    private final TaskMissPolicy missPolicy;

    public UserTask(
            TaskFn taskFn,
            Object arg,
            long intervalNs,
            TaskMissPolicy missPolicy
    ) {
        this.taskFn = Objects.requireNonNull(taskFn, "taskFn");
        this.arg = arg;
        this.intervalNs = intervalNs;
        this.missPolicy = Objects.requireNonNull(missPolicy, "missPolicy");
    }

    public Object run() {
        return taskFn.run(arg);
    }

    public TaskFn taskFn() {
        return taskFn;
    }

    public Object arg() {
        return arg;
    }

    public long intervalNs() {
        return intervalNs;
    }

    public TaskMissPolicy missPolicy() {
        return missPolicy;
    }
}
