package com.github.hippalus.hbase.testutil.cluster;

import com.github.hippalus.hbase.util.ExecutorUtils;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;

public class InMemoryHBaseCluster implements HBaseCluster {

  private final static long CLUSTER_DEFAULT_TIMEOUT = 60;
  private final HBaseTestingUtility utility;
  private final ExecutorService executorService;
  private final long timeout;
  private Connection connection;

  public InMemoryHBaseCluster() {
    this(CLUSTER_DEFAULT_TIMEOUT);
  }

  public InMemoryHBaseCluster(long timeout) {
    this.utility = new HBaseTestingUtility();
    this.executorService = Executors.newSingleThreadExecutor();
    this.timeout = timeout;
  }

  @Override
  public Connection start() throws IOException {
    try {
      executorService.submit(() -> utility.startMiniCluster(1, 1, false)).get(timeout, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException();
    }
    connection = utility.getConnection();
    if (this.connection == null) {
      throw new IOException();
    }
    return connection;
  }

  @Override
  public void end() throws Exception {
    connection.close();
    utility.shutdownMiniCluster();
    ExecutorUtils.gracefulShutdown(1,TimeUnit.MINUTES,executorService);
  }
}
