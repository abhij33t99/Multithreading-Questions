package Uber.scheduled_executor.model;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ScheduledTask implements Comparable<ScheduledTask> {

    private final long seqNo;
    private final Runnable runnable;
    private final TaskType taskType;
    private final long intervalNanos;
    private final AtomicBoolean cancelled;

    long nextRunTimeNanos; // The time (in nanos) this should run next
    long anchorTimeNanos;  // For fixed-rate, to avoid drift

    public ScheduledTask(Runnable runnable, TaskType taskType, long intervalNanos, long nextRunTimeNanos, long anchorTimeNanos, AtomicLong sequence) {
        this.runnable = runnable;
        this.taskType = taskType;
        this.intervalNanos = intervalNanos;
        this.cancelled = new AtomicBoolean(false);
        this.nextRunTimeNanos = nextRunTimeNanos;
        this.anchorTimeNanos = anchorTimeNanos;
        this.seqNo = sequence.incrementAndGet();
    }


    @Override
    public int compareTo(ScheduledTask o) {
        if (this.nextRunTimeNanos == o.nextRunTimeNanos) {
            return Long.compare(this.seqNo, o.seqNo);
        }
        return Long.compare(this.nextRunTimeNanos, o.nextRunTimeNanos);
    }

    public void setNextRunTimeNanos(long nextRunTimeNanos) {
        this.nextRunTimeNanos = nextRunTimeNanos;
    }

    public void setAnchorTimeNanos(long anchorTimeNanos) {
        this.anchorTimeNanos = anchorTimeNanos;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public Runnable getRunnable() {
        return runnable;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public long getIntervalNanos() {
        return intervalNanos;
    }

    public boolean isCancelled() {
        return cancelled.get();
    }

    public long getNextRunTimeNanos() {
        return nextRunTimeNanos;
    }

    public long getAnchorTimeNanos() {
        return anchorTimeNanos;
    }

    public AtomicBoolean getHandle() {
        return cancelled;
    }
}
