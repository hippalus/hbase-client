package com.github.hippalus.hbase.common.factory;

import com.github.hippalus.hbase.common.HBaseCommandOperations;
import org.apache.hadoop.hbase.client.Connection;


public interface HBaseCommandOperationsFactory extends AutoCloseable {

  HBaseCommandOperations create(Connection connection);

  @Override
  default void close() {
  }
}
