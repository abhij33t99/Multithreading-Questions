package Uber;

import Uber.executor.CustomScheduledExecutor;
import Uber.model.TaskType;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ScheduledExecutorService {

    private final CustomScheduledExecutor executor;

    public ScheduledExecutorService(int poolSize) {
        executor = new CustomScheduledExecutor(poolSize);
    }

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     */
    public AtomicBoolean schedule(Runnable command, long delay, TimeUnit unit) {
        return executor.enqueueTask(command, TaskType.ONE_SHOT, unit.toNanos(delay), 0);
    }

    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and
     * subsequently with the given period; that is, executions will commence after initialDelay then
     * initialDelay+period, then initialDelay + 2 * period, and so on.
     */
    public AtomicBoolean scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return executor.enqueueTask(command, TaskType.FIXED_RATE, unit.toNanos(initialDelay), unit.toNanos(period));
    }

    /*
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and
     * subsequently with the given delay between the termination of one execution and the commencement of the next.
     */
    public AtomicBoolean scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return executor.enqueueTask(command, TaskType.FIXED_DELAY, unit.toNanos(initialDelay), unit.toNanos(delay));
    }

    public void shutDown() {
        executor.shutdown();
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting main application.");

        // 1. Create the executor with 4 worker threads
        ScheduledExecutorService executorService = new ScheduledExecutorService(4);

        // --- 2. Schedule (One-Shot) ---
        System.out.println("Scheduling ONE-SHOT task for 2 seconds from now.");
        executorService.schedule(() -> {
            System.out.println("SUCCESS: ONE-SHOT task executed.");
        }, 2, TimeUnit.SECONDS);


        // --- 3. Schedule at Fixed Rate ---
        System.out.println("Scheduling FIXED-RATE task to run every 3 seconds, starting in 1s.");
        AtomicInteger rateCounter = new AtomicInteger(0);
        AtomicBoolean rateHandle = executorService.scheduleAtFixedRate(() -> {
            int count = rateCounter.incrementAndGet();
            System.out.println("FIXED-RATE task running... (Execution " + count + ")");
            // This task is fast, so it should run close to every 3s
        }, 1, 3, TimeUnit.SECONDS);


        // --- 4. Schedule with Fixed Delay ---
        System.out.println("Scheduling FIXED-DELAY task to run every 5s *after* completion, starting in 2s.");
        AtomicInteger delayCounter = new AtomicInteger(0);

        executorService.scheduleWithFixedDelay(() -> {
            int count = delayCounter.incrementAndGet();
            System.out.println("FIXED-DELAY task running... (Execution " + count + ")");
            // Simulate work that takes 2 seconds
            System.out.println("FIXED-DELAY task: starting 2s of work...");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                System.out.println("FIXED-DELAY task was interrupted.");
            }
            System.out.println("FIXED-DELAY task: ...work finished.");
            // The 5s delay will start *after* this log message
        }, 2, 5, TimeUnit.SECONDS);


        // --- 5. Let the tasks run for a while ---
        System.out.println("Main thread sleeping for 15 seconds to observe tasks...");
        Thread.sleep(15000);


        // --- 6. Demonstrate Cancellation ---
        System.out.println("Cancelling the FIXED-RATE task.");
        rateHandle.compareAndSet(false, true);
        System.out.println("FIXED-RATE task cancelled. It should not run anymore.");

        // --- 7. Shut down the executor ---
        System.out.println("Main thread sleeping for 10 more seconds...");
        Thread.sleep(10000);

        System.out.println("Shutting down the executor.");
        executorService.shutDown();
        System.out.println("Executor shutdown initiated.");
        System.out.println("Main thread finished.");
    }

}
