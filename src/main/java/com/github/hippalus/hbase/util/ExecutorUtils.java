package com.github.hippalus.hbase.util;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j;

@Log4j
public final class ExecutorUtils {


  private ExecutorUtils() {
    throw new AssertionError();
  }


  public static void gracefulShutdown(long timeout, TimeUnit unit, ExecutorService... executorServices) {
    for (ExecutorService executorService : executorServices) {
      executorService.shutdown();
    }
    boolean wasInterrupted = false;
    final long endTime = unit.toMillis(timeout) + System.currentTimeMillis();
    long timeLeft = unit.toMillis(timeout);
    boolean hasTimeLeft = timeLeft > 0L;

    for (ExecutorService executorService : executorServices) {
      if (wasInterrupted || !hasTimeLeft) {
        executorService.shutdownNow();
      } else {
        try {
          if (!executorService.awaitTermination(timeLeft, TimeUnit.MILLISECONDS)) {
            log.warn("ExecutorService did not terminate in time. Shutting it down now.");
            executorService.shutdownNow();
          }
        } catch (InterruptedException e) {
          log.warn("Interrupted while shutting down executor services. Shutting all remaining ExecutorServices down now.", e);
          executorService.shutdownNow();

          wasInterrupted = true;

          Thread.currentThread().interrupt();
        }
        timeLeft = endTime - System.currentTimeMillis();
        hasTimeLeft = timeLeft > 0L;
      }
    }
  }

}
