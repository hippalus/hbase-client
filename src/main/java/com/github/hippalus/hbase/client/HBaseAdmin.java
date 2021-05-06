package com.github.hippalus.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

@Log4j2
@ThreadSafe
public class HBaseAdmin {

  private static volatile HBaseAdmin instance;
  private final Connection connection;

  private HBaseAdmin(Connection connection) {
    this.connection = connection;
  }

  public static HBaseAdmin getInstance(Connection connection) {
    HBaseAdmin hbcf = instance;
    if (hbcf == null) {
      synchronized (HBaseAdmin.class) {
        hbcf = instance;
        if (hbcf == null) {
          hbcf = instance = new HBaseAdmin(connection);
        }
      }
    }
    return hbcf;
  }

  public Connection getConnection() {
    return connection;
  }


  public Admin getAdmin() throws IOException {
    return connection.getAdmin();
  }

  public List<ServerName> getServers() throws IOException {
    try (Admin admin = getAdmin()) {
      return new ArrayList<>(admin.getRegionServers());
    }
  }

  public ServerName getMasterServer() throws IOException {
    try (Admin admin = getAdmin()) {
      return admin.getMaster();
    }
  }

  public void createNamespace(String namespace) throws IOException {
    try (Admin admin = getAdmin()) {
      admin.createNamespace(NamespaceDescriptor.create(namespace).build());
    }
  }

  public void createTable(Map<TableName, List<String>> tableNameFamilyPair) throws IOException {
    try (Admin admin = getAdmin()) {
      List<TableDescriptorBuilder> tableDescriptorBuilderList = tableNameFamilyPair.entrySet()
          .stream()
          .map(this::getTableDescriptorBuilder)
          .collect(Collectors.toList());

      for (TableDescriptorBuilder tableDescBuilder : tableDescriptorBuilderList) {
        this.createOrOverwrite(admin, tableDescBuilder.build());
      }
    }
  }

  private TableDescriptorBuilder getTableDescriptorBuilder(Map.Entry<TableName, List<String>> tblAndFamily) {
    TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tblAndFamily.getKey());
    for (String family : tblAndFamily.getValue()) {
      tableDescBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family))
          .setMaxVersions(1) //TODO  configurable
          .build());
    }
    return tableDescBuilder;
  }


  public void createOrOverwrite(Admin admin, TableDescriptor tableDescriptor) throws IOException {
    if (admin.tableExists(tableDescriptor.getTableName())) {
      admin.disableTable(tableDescriptor.getTableName());
      admin.deleteTable(tableDescriptor.getTableName());
    }
    admin.createTable(tableDescriptor);
  }

  public void deleteTable(TableName tableName) throws IOException {
    try (Admin admin = getAdmin()) {
      admin.deleteTable(tableName);
    }
  }

  public void enableTable(TableName tableName) throws IOException {
    try (Admin admin = getAdmin()) {
      admin.enableTable(tableName);
    }
  }

  public void disableTable(TableName tableName) throws IOException {
    try (Admin admin = getAdmin()) {
      admin.disableTable(tableName);
    }
  }

  public boolean tableExists(TableName tableName) throws IOException {
    try (Admin admin = getAdmin()) {
      return admin.tableExists(tableName);
    }
  }
}
