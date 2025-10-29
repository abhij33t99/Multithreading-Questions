package Uber.executor;

import Uber.model.ScheduledTask;
import Uber.model.TaskType;

import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class CustomScheduledExecutor {

    private final ExecutorService workerPool;
    private final Thread timerThread;
    private final PriorityQueue<ScheduledTask> taskQueue;
    private final ReentrantLock queueLock;
    private final Condition newTaskCondition;
    private final AtomicBoolean stop;
    private final AtomicLong sequence;

    public CustomScheduledExecutor(int poolSize) {
        this.workerPool = Executors.newFixedThreadPool(poolSize, new CustomThreadFactory());
        this.taskQueue = new PriorityQueue<>();
        this.queueLock = new ReentrantLock();
        this.newTaskCondition = queueLock.newCondition();
        this.stop = new AtomicBoolean(false);
        this.sequence = new AtomicLong(0);

        this.timerThread = new Thread(this::timerLoop);
        this.timerThread.start();
    }

    // Timer thread to take tasks from the queue and send them to the pool for processing
    private void timerLoop() {
        while (!stop.get()) {
            ScheduledTask taskToRun = null;
            queueLock.lock();
            try {
                if (stop.get()) break; // Check stop flag *inside* lock

                if (taskQueue.isEmpty()) {
                    // Queue is empty, wait indefinitely for a signal
                    try {
                        newTaskCondition.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // Restore interrupt status
                        break; // Exit the loop on interrupt
                    }
                    // Loop again to re-check queue
                    continue;
                }

                // Queue is not empty, peek at the top task
                ScheduledTask top = taskQueue.peek();
                long now = System.nanoTime();
                long delayNanos = top.getNextRunTimeNanos() - now;

                if (delayNanos <= 0) {
                    // Time to run!
                    taskToRun = taskQueue.poll();
                } else {
                    // Not time yet, wait for the specific duration.
                    // awaitNanos will wake up if signalled or time elapses.
                    try {
                        newTaskCondition.awaitNanos(delayNanos);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // Restore interrupt status
                        break; // Exit the loop on interrupt
                    }
                    // After waiting, loop again (continue) to re-evaluate
                    // the queue. A new, earlier task might have been added.
                }
            } finally {
                queueLock.unlock();
            }

            // Process the task *outside* the lock
            if (taskToRun != null) {
                processTask(taskToRun);
            }
        }
    }

    private void processTask(ScheduledTask task) {
        if (task.isCancelled()) {
            return;
        }

        switch (task.getTaskType()) {
            case ONE_SHOT -> workerPool.submit(task.getRunnable());

            case FIXED_RATE -> {
                workerPool.submit(task.getRunnable());

                long now = System.nanoTime();
                do {
                    task.setAnchorTimeNanos(task.getAnchorTimeNanos() + task.getIntervalNanos());
                } while (task.getAnchorTimeNanos() <= now);

                task.setNextRunTimeNanos(task.getAnchorTimeNanos());

                requeueTask(task);
            }

            case FIXED_DELAY -> {

                Runnable fixedDelayWrapper = () -> {
                    try {
                        task.getRunnable().run();
                    } catch (Throwable t) {
                        System.err.println("Uncaught exception/error in fixed-delay task: " + t.getMessage());
                    } finally {
                        if (task.isCancelled() || stop.get()) return;

                        task.setNextRunTimeNanos(System.nanoTime() + task.getIntervalNanos());
                        requeueTask(task);
                    }
                };

                workerPool.submit(fixedDelayWrapper);
            }
        }
    }

    private void requeueTask(ScheduledTask task) {
        if (task.isCancelled() || stop.get()) return;
        queueLock.lock();
        try {
            taskQueue.add(task);
            newTaskCondition.signal();
        } finally {
            queueLock.unlock();
        }
    }

    public AtomicBoolean enqueueTask(Runnable r, TaskType taskType, long initialDelayNanos, long intervalNanos) {
        if (stop.get()) {
            throw new IllegalStateException("Executor has been stopped");
        }

        long now = System.nanoTime();
        ScheduledTask task = new ScheduledTask(
                r,
                taskType,
                intervalNanos,
                now + initialDelayNanos,
                now + initialDelayNanos,
                sequence
        );

        requeueTask(task);
        return task.getHandle();
    }

    public void shutdown() {
        if (!stop.compareAndSet(false, true))
            return;

        queueLock.lock();
        try {
            newTaskCondition.signalAll();
        } finally {
            queueLock.unlock();
        }

        try {
            timerThread.interrupt();
            timerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        workerPool.shutdown();

        try {
            if (!workerPool.awaitTermination(60, TimeUnit.SECONDS)) {
                workerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            workerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
