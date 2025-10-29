package Uber.executor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadFactory implements ThreadFactory {

    private final AtomicInteger threadCounter = new AtomicInteger(0);

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "Worker Thread" + threadCounter.incrementAndGet());
        t.setUncaughtExceptionHandler((thread, throwable) -> {
            System.out.println("Exception in thread " + thread.getName() + ":" + throwable.getMessage());
        });

        return t;
    }
}
