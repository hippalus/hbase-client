package com.github.hippalus.hbase.common.factory.impl;

import com.google.auto.service.AutoService;
import com.github.hippalus.hbase.common.HBaseQueryOperations;
import com.github.hippalus.hbase.common.HBaseQueryOperationsImpl;
import com.github.hippalus.hbase.common.factory.HBaseQueryOperationsFactory;
import org.apache.hadoop.hbase.client.Connection;


@AutoService(HBaseQueryOperationsFactory.class)
public final class HBaseQueryOperationsFactoryImpl implements HBaseQueryOperationsFactory {

  @Override
  public HBaseQueryOperations create(Connection connection) {
    return new HBaseQueryOperationsImpl(connection);
  }
}
