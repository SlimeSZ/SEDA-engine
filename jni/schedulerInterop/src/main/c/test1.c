#include <jni.h>
#include <stdint.h>
#include <stdlib.h>
#include "com_scheduler_Main.h"
#include "scheduler/scheduler.h"
#include "scheduler/scheduler_state.h"
#include "utils/async_queue.h"

/*	Symbolic Link only as of right now (fix later)
ln -s ~/C/SEDA/scheduler ~/java/schedulerInterop/src/main/c/scheduler
ln -s ~/C/SEDA/utils    ~/java/schedulerInterop/src/main/c/utils 
*/
#include "scheduler/scheduler.h"
#include "utils/async_queue.h"

JNIEXPORT jlong JNICALL Java_com_scheduler_Main_outQueueInit(
	JNIEnv *env, jclass cls, jint capacity
) {
	async_queue_t *out_q = async_queue_init(100);
	if (!out_q) return 0;
	return (jlong)(uintptr_t)out_q;	
} 

JNIEXPORT jlong JNICALL Java_com_scheduler_Main_schedulerInit(
	JNIEnv *env, jclass cls, 
	jint workers, jint tasks, jlong outQ 
) {
	async_queue_t *out_q = (async_queue_t*)(uintptr_t)outQ;
	scheduler_t *sched = scheduler_init(
		workers, tasks, out_q
	); 
	if (!sched) 
		return 0;
	return (jlong)(uintptr_t)sched;
}



// stored per-task, holds the Java callback
typedef struct {
    JavaVM  *jvm;
    jobject  task_obj;  // global ref to UserTask instance
} java_task_ctx_t;

// C scheduler calls this — it calls back into Java
static void *java_trampoline(void *arg) {
    java_task_ctx_t *ctx = (java_task_ctx_t *)arg;
    JNIEnv *env;
    (*ctx->jvm)->AttachCurrentThread(ctx->jvm, (void **)&env, NULL);

    jclass task_class = (*env)->GetObjectClass(env, ctx->task_obj);
    jmethodID run_mid = (*env)->GetMethodID(env, task_class, "run", "()Ljava/lang/Object;");
    (*env)->CallObjectMethod(env, ctx->task_obj, run_mid);
    return NULL;
}

JNIEXPORT jintArray JNICALL Java_com_scheduler_Main_addTasks(
    JNIEnv *env, jclass cls,
    jlong schedPtr, jobjectArray tasks, jint numTasks
) {
    scheduler_t *s = (scheduler_t *)(uintptr_t)schedPtr;
    JavaVM *jvm;
    (*env)->GetJavaVM(env, &jvm);

    // get UserTask field method IDs once
    jclass task_class = (*env)->GetObjectClass(env,
        (*env)->GetObjectArrayElement(env, tasks, 0));
    jmethodID get_interval = (*env)->GetMethodID(env, task_class, "intervalNs", "()J");
    jmethodID get_policy   = (*env)->GetMethodID(env, task_class, "missPolicy",  "()Lcom/scheduler/TaskMissPolicy;");

    jclass policy_class    = (*env)->FindClass(env, "com/scheduler/TaskMissPolicy");
    jmethodID to_native    = (*env)->GetMethodID(env, policy_class, "toNative", "()I");

    user_task_t *user_tasks = malloc((size_t)numTasks * sizeof(user_task_t));
    java_task_ctx_t *ctxs   = malloc((size_t)numTasks * sizeof(java_task_ctx_t));
    if (!user_tasks || !ctxs) return NULL;

    for (int i = 0; i < numTasks; i++) {
        jobject task_obj = (*env)->GetObjectArrayElement(env, tasks, i);

        ctxs[i].jvm      = jvm;
        ctxs[i].task_obj = (*env)->NewGlobalRef(env, task_obj);  // survives GC

        jlong interval   = (*env)->CallLongMethod(env, task_obj, get_interval);
        jobject policy   = (*env)->CallObjectMethod(env, task_obj, get_policy);
        jint    policy_i = (*env)->CallIntMethod(env, policy, to_native);

        user_tasks[i].task_fn     = java_trampoline;
        user_tasks[i].arg         = &ctxs[i];
        user_tasks[i].interval_ns = (uint64_t)interval;
        user_tasks[i].miss_policy = (task_miss_policy_t)policy_i;
        user_tasks[i].id          = 0; // assigned by scheduler
    }

    int *ids = scheduler_add_tasks(s, user_tasks, (size_t)numTasks);
    free(user_tasks);

    if (!ids) { free(ctxs); return NULL; }

    // pack ids into jintArray to return to Java
    jintArray result = (*env)->NewIntArray(env, numTasks);
    (*env)->SetIntArrayRegion(env, result, 0, numTasks, (jint *)ids);
    free(ids);

    return result;
}

JNIEXPORT void JNICALL Java_com_scheduler_Main_schedulerShutdown(
	JNIEnv *env, jclass cls, jlong ptr 
) {
	scheduler_t *sched = (scheduler_t*)(uintptr_t)ptr;
	scheduler_shutdown(sched);	
}

JNIEXPORT void JNICALL Java_com_scheduler_Main_schedulerRun(
	JNIEnv *env, jclass cls, jlong ptr 
) {
	scheduler_t *sched = (scheduler_t*)(uintptr_t)ptr;
	scheduler_run(sched);	
}

JNIEXPORT jint JNICALL Java_com_scheduler_Main_removeTask(
    JNIEnv *env, jclass cls, jlong schedPtr, jint taskId
) {
    scheduler_t *s = (scheduler_t *)(uintptr_t)schedPtr;
    return (jint)scheduler_remove_task(s, (int)taskId);
}

/*
Compile with:
gcc -O3 -fPIC \
    -I"$JAVA_HOME/include" \
    -I"$JAVA_HOME/include/linux" \
    -I"$HOME/C/SEDA" \
    -shared -o src/main/c/libschedulerInterop.so \
    src/main/c/test1.c \
    ~/C/SEDA/scheduler/scheduler.c \
    ~/C/SEDA/scheduler/scheduler_state.c \
    ~/C/SEDA/utils/async_queue.c \
    ~/C/SEDA/utils/worker_pool.c
*/
