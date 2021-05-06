package com.github.hippalus.hbase.common.factory;

import com.github.hippalus.hbase.common.HBaseQueryOperations;
import org.apache.hadoop.hbase.client.Connection;

public interface HBaseQueryOperationsFactory extends AutoCloseable {

  HBaseQueryOperations create(Connection connection);

  @Override
  default void close() {

  }
}
