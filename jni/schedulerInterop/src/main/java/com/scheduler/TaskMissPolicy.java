package com.scheduler;

public enum TaskMissPolicy {
    TASK_MISS_SKIP(0),        // discard missed executions
    TASK_MISS_COALESCE(1),    // collapse missed executions into one
    TASK_MISS_CATCHUP(2);     // replay all missed executions

    private final int value;

    TaskMissPolicy(int value) {
        this.value = value;
    }

    public int toNative() {
        return value;
    }

    public static TaskMissPolicy fromNative(int value) {
        switch (value) {
            case 0: return TASK_MISS_SKIP;
            case 1: return TASK_MISS_COALESCE;
            case 2: return TASK_MISS_CATCHUP;
            default:
                throw new IllegalArgumentException("Invalid task_miss_policy_t: " + value);
        }
    }
}
