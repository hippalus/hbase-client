package com.github.hippalus.hbase.util;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class FutureUtils {

  private FutureUtils() {
    throw new AssertionError();
  }

  public static <T> void addListener(CompletableFuture<T> future, BiConsumer<? super T, ? super Throwable> action) {
    future.whenComplete((resp, error) -> {
      try {
        action.accept(resp, unwrapCompletionException(error));
      } catch (Throwable t) {
        log.error("Unexpected error caught when processing CompletableFuture", t);
      }
    });
  }

  private static Throwable unwrapCompletionException(Throwable error) {
    if (error instanceof CompletionException) {
      Throwable cause = error.getCause();
      if (cause != null) {
        return cause;
      }
    }
    return error;
  }
}
