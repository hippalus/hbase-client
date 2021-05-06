package com.github.hippalus.hbase.common;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;

@Slf4j
public class HBaseCommandOperationsImpl implements HBaseCommandOperations {

  private final Connection writeConnection;

  public HBaseCommandOperationsImpl(Connection connection) {
    this.writeConnection = connection;
  }

  @Override
  public Connection getConnection() {
    return this.writeConnection;
  }

  @Override
  public void warmUpConnectionCache(TableName tableName) throws IOException {
    try (RegionLocator locator = writeConnection.getRegionLocator(tableName)) {
      log.info("Warmed up region location cache for {} got {} ", tableName, locator.getAllRegionLocations().size());
    }
  }

  @Override
  public boolean put(TableName tableName, final String rowKey, final FamilyAndColumn familyAndColumn, final byte[] data)
      throws IOException {
    try (Table t = getTable(tableName)) {
      t.put(this.getPut(rowKey, familyAndColumn, data));
      return true;
    }
  }

  @Override
  public boolean put(TableName tableName, final Map<String, Map<FamilyAndColumn, byte[]>> rows) throws IOException {
    try (Table t = getTable(tableName)) {
      final List<Put> puts = new ArrayList<>(rows.size());
      for (Map.Entry<String, Map<FamilyAndColumn, byte[]>> entry : rows.entrySet()) {
        puts.add(this.getPut(entry.getKey(), entry.getValue()));
      }
      t.put(puts);
      return true;
    }
  }

  @Override
  public boolean put(TableName tableName, final FamilyAndColumn familyAndColumn, final Map<String, byte[]> data)
      throws IOException {
    try (Table t = getTable(tableName)) {
      final List<Put> puts = new ArrayList<>();
      for (Map.Entry<String, byte[]> entry : data.entrySet()) {
        puts.add(this.getPut(entry.getKey(), familyAndColumn, entry.getValue()));
      }
      t.put(puts);
      return true;
    }
  }

  private Put getPut(String rowKey, Map<FamilyAndColumn, byte[]> cells) throws IOException {
    final byte[] rk = toBytes(rowKey);
    Put p = new Put(rk);
    for (Map.Entry<FamilyAndColumn, byte[]> entry : cells.entrySet()) {
      p.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          .setRow(rk)
          .setFamily(entry.getKey().getFamilyBytes())
          .setQualifier(entry.getKey().getColumnBytes())
          .setTimestamp(p.getTimestamp())
          .setType(Cell.Type.Put)
          .setValue(entry.getValue())
          .build());
    }
    return p;
  }

  private Put getPut(String rowKey, FamilyAndColumn familyAndColumn, byte[] data) throws IOException {
    final byte[] rk = toBytes(rowKey);
    Put p = new Put(rk);
    p.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
        .setRow(rk)
        .setFamily(familyAndColumn.getFamilyBytes())
        .setQualifier(familyAndColumn.getColumnBytes())
        .setTimestamp(p.getTimestamp())
        .setType(Cell.Type.Put)
        .setValue(data)
        .build());
    return p;
  }

  @Override
  public boolean delete(TableName tableName, final String rowKey, final FamilyAndColumn familyAndColumn)
      throws IOException {
    try (Table t = getTable(tableName)) {
      t.delete(getDelete(rowKey, familyAndColumn));
      return true;
    }
  }

  private Delete getDelete(String rowKey, FamilyAndColumn familyAndColumn) {
    Delete delete = new Delete(toBytes(rowKey));
    createDeleteDetail(delete, familyAndColumn);
    return delete;
  }

  private Delete getDelete(String rowKey, List<FamilyAndColumn> familyAndColumn) {
    Delete delete = new Delete(toBytes(rowKey));
    for (FamilyAndColumn pair : familyAndColumn) {
      createDeleteDetail(delete, pair);
    }
    return delete;
  }

  private void createDeleteDetail(Delete delete, FamilyAndColumn pair) {
    byte[] family = pair.getFamilyBytes();
    if (pair.columnIsNonNull()) {
      delete.addColumn(family, pair.getColumnBytes());
    } else {
      delete.addFamily(family);
    }
  }

  @Override
  public boolean delete(TableName tableName, final List<FamilyAndColumn> familyAndColumns, final List<String> rowKeys)
      throws IOException {
    List<Delete> deletes = new ArrayList<>(rowKeys.size());
    for (String rowKey : rowKeys) {
      deletes.add(getDelete(rowKey, familyAndColumns));
    }
    try (Table table = getTable(tableName)) {
      table.delete(deletes);
    }
    return true;
  }

  private Table getTable(TableName tableName) throws IOException {
    return writeConnection.getTable(tableName);
  }
}
