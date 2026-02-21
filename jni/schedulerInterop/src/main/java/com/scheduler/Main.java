package com.scheduler;

public class Main {

	static int numWorkers = 20;
	static int numTasks = 5;

	static { System.loadLibrary("schedulerInterop"); } 


	public static native long outQueueInit(int capacity);
	public static native long schedulerInit(int workers, int tasks, long outqPtr); 
	public static native int []addTasks(long schedPtr, UserTask[] tasks, int numTasks);
	public static native int removeTask(long schedPtr, int taskId);
	public static native void schedulerRun(long ptr);
	public static native void schedulerShutdown(long ptr);

	public static void main(String[] args) {
		long outQ = outQueueInit(100);
		if (outQ == 0) {
			System.out.println("init failed (outQ)");
			return;
		}
		long sched = schedulerInit(numWorkers, numTasks, outQ);
		if (sched == 0) {
			System.out.println("init failed (sched)");
			return;
		}

		UserTask[] userTasks = init_dummies();	
		int[] task_ids = addTasks(sched, userTasks, numTasks);
		
		for (int id : task_ids) 
			System.out.println("Task Id: " + id);

		final long schedRef = sched;
		new Thread(() -> {
			try {
				Thread.sleep(3000);
				System.out.println("[java] removed id=" + task_ids[0]);
				removeTask(schedRef, task_ids[0]);
				Thread.sleep(3000);
				schedulerShutdown(schedRef);
			} catch (InterruptedException e) {}
		}).start();

		schedulerRun(sched);
	}
	




	public static UserTask[] init_dummies() {
		UserTask[] userTasks = {

    new UserTask(
        (arg) -> {
            System.out.println("Task 1 -> " + arg);
            return null;
        },
        "alpha",
        500_000_000L, // 500 ms
        TaskMissPolicy.TASK_MISS_SKIP
    ),

    new UserTask(
        (arg) -> {
            int x = (int) arg;
            int result = x * x;
            System.out.println("Task 2 squared = " + result);
            return result;
        },
        7,
        1_000_000_000L, // 1 sec
        TaskMissPolicy.TASK_MISS_COALESCE
    ),

    new UserTask(
        (arg) -> {
            System.out.println("Task 3 timestamp = " + System.nanoTime());
            return null;
        },
        null,
        250_000_000L, // 250 ms
        TaskMissPolicy.TASK_MISS_CATCHUP
    ),

    new UserTask(
        (arg) -> {
            double v = (double) arg;
            double out = Math.sqrt(v);
            System.out.println("Task 4 sqrt = " + out);
            return out;
        },
        144.0,
        750_000_000L, // 750 ms
        TaskMissPolicy.TASK_MISS_SKIP
    ),

    new UserTask(
        (arg) -> {
            String s = (String) arg;
            String upper = s.toUpperCase();
            System.out.println("Task 5 upper = " + upper);
            return upper;
        },
        "jni",
        2_000_000_000L, // 2 sec
        TaskMissPolicy.TASK_MISS_COALESCE
    )
};
	return userTasks;
	}

	
}
