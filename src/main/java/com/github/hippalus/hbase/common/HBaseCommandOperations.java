package com.github.hippalus.hbase.common;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;

public interface HBaseCommandOperations extends HBaseOperations {

  boolean put(TableName tableName, final String rowKey, final FamilyAndColumn familyAndColumn, final byte[] data)
      throws IOException;

  boolean put(TableName tableName, final FamilyAndColumn familyAndColumn, final Map<String, byte[]> data)
      throws IOException;

  boolean put(TableName tableName, final Map<String, Map<FamilyAndColumn, byte[]>> rows) throws IOException;

  boolean delete(TableName tableName, final String rowKey, final FamilyAndColumn familyAndColumn) throws IOException;

  boolean delete(TableName tableName, final List<FamilyAndColumn> familyAndColumns, final List<String> rowKeys)
      throws IOException;
}
