package com.gboissinot.devinci.streaming.data.module.collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Gregory Boissinot
 */
class ScheduledExecutorRepeat {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledExecutorRepeat.class);

    private final AtomicInteger counter = new AtomicInteger(0);

    private final Collector collector;
    private final int maxRepeat;

    ScheduledExecutorRepeat(Collector collector, int maxRepeat) {
        this.collector = collector;
        this.maxRepeat = maxRepeat;
    }

    void repeat() throws InterruptedException {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

        SchedulingTask scheduledTask = new SchedulingTask();
        ScheduledFuture<?> scheduledFuture = executorService.scheduleAtFixedRate(scheduledTask, 5, 30, TimeUnit.SECONDS);

        while (true) {
            logger.info("Counting : " + counter.get());
            Thread.sleep(1000);
            if (counter.get() == maxRepeat) {
                logger.debug("Count is " + maxRepeat + ", cancel the scheduledFuture!");
                scheduledFuture.cancel(true);
                executorService.shutdown();
                break;
            }
        }
    }

    private class SchedulingTask implements Runnable {
        @Override
        public void run() {
            try {
                counter.incrementAndGet();
                collector.collect();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }
}
