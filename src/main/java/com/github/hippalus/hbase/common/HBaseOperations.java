package com.github.hippalus.hbase.common;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

public interface HBaseOperations extends AutoCloseable {

  static Charset getCharSet() {
    return StandardCharsets.UTF_8;
  }

  @Override
  default void close() throws Exception {
    if (getConnection() != null && getConnection().isClosed()) {
      getConnection().close();
    }
  }

  Connection getConnection();

  void warmUpConnectionCache(TableName tableName) throws IOException;

}
