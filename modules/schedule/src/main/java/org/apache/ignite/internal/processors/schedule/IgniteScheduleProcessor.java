package org.apache.ignite.internal.processors.schedule;

import it.sauronsoftware.cron4j.Scheduler;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.scheduler.SchedulerFuture;
import org.jetbrains.annotations.Nullable;
import java.util.concurrent.ConcurrentHashMap;

public class IgniteScheduleProcessor extends IgniteScheduleProcessorAdapter {
    /** Cron scheduler. */
    private Scheduler sched;

    /** Schedule futures. */
    //private Set<SchedulerFuture<?>> schedFuts = new GridConcurrentHashSet<>();
    private ConcurrentHashMap<String, SchedulerFuture<?>> schedFuts = new ConcurrentHashMap<String, SchedulerFuture<?>>();

    /**
     * @param ctx Kernal context.
     */
    public IgniteScheduleProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public SchedulerFuture<?> schedule(final String name, final Runnable c, String ptrn) {
        assert c != null;
        assert ptrn != null;

        ScheduleFutureImpl<Object> fut = new ScheduleFutureImpl<>(name, sched, ctx, ptrn);

        fut.schedule(new IgniteCallable<Object>() {
            @Nullable @Override public Object call() {
                c.run();

                return null;
            }
        });

        return fut;
    }

    /** {@inheritDoc} */
    @Override public <R> SchedulerFuture<R> schedule(final String name, final  Callable<R> c, final String pattern) {
        assert c != null;
        assert pattern != null;

        ScheduleFutureImpl<R> fut = new ScheduleFutureImpl<>(name, sched, ctx, pattern);

        fut.schedule(c);

        return fut;
    }

    /**
     *
     * @return Future objects of currently scheduled active(not finished) tasks.
     */
    public ConcurrentHashMap<String, SchedulerFuture<?>> getScheduledFutures() {
        return this.schedFuts;
    }
//    public Collection<SchedulerFuture<?>> getScheduledFutures() {
//        return Collections.unmodifiableList(new ArrayList<>(schedFuts));
//    }

    /**
     * Removes future object from the collection of scheduled futures.
     *
     * @param funName
     */
    public void onDescheduled(final String funName) {
        schedFuts.remove(funName);
    }

    /**
     * Adds future object to the collection of scheduled futures.
     *
     * @param fut Future object.
     */
    public void onScheduled(final String funName, final SchedulerFuture<?> fut) {
        assert fut != null;
        schedFuts.put(funName, fut);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        sched = new Scheduler();

        if (sched.isStarted() == false) {
            sched.start();
            X.println(">>>");
            X.println("定时任务启动！");
            X.println(">>>");
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (sched.isStarted())
            sched.stop();

        sched = null;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Schedule processor memory stats [igniteInstanceName=" + ctx.igniteInstanceName() + ']');
        X.println(">>>   schedFutsSize: " + schedFuts.size());
    }
}