package com.github.hippalus.hbase.common.factory.impl;

import com.google.auto.service.AutoService;
import com.github.hippalus.hbase.common.HBaseCommandOperations;
import com.github.hippalus.hbase.common.HBaseCommandOperationsImpl;
import com.github.hippalus.hbase.common.factory.HBaseCommandOperationsFactory;
import org.apache.hadoop.hbase.client.Connection;


@AutoService(HBaseCommandOperationsFactory.class)
public final class HBaseCommandOperationsFactoryImpl implements HBaseCommandOperationsFactory {

  @Override
  public HBaseCommandOperations create(Connection connection) {
    return new HBaseCommandOperationsImpl(connection);
  }
}
