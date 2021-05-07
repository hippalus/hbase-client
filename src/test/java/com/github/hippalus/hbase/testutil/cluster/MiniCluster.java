package com.github.hippalus.hbase.testutil.cluster;

import com.github.hippalus.hbase.client.HBaseAdmin;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

@Getter
public class MiniCluster {

  private final Connection connection;
  private final HBaseCluster hBaseCluster;
  private final HBaseAdmin hBaseAdmin;

  @SneakyThrows
  private MiniCluster() {
    hBaseCluster = new InMemoryHBaseCluster();
    connection = hBaseCluster.start();
    hBaseAdmin = HBaseAdmin.getInstance(connection);
  }

  public static MiniCluster startup() {
    return new MiniCluster();
  }


  @SneakyThrows
  public void close() {
    connection.close();
    hBaseCluster.end();
  }

  public void createTables(Map<TableName, List<String>> tableNameFamilyPair) throws IOException {
    System.out.format("Creating table '%s': ", tableNameFamilyPair.keySet().toString());
    hBaseAdmin.createTable(tableNameFamilyPair);
    System.out.println("[DONE]");
  }

  public void deleteTables(TableName... tableNames) throws IOException {
    for (TableName tblName : tableNames) {
      if (hBaseAdmin.tableExists(tblName)) {
        System.out.format("Deleting table '%s': ", tblName);
        hBaseAdmin.disableTable(tblName);
        hBaseAdmin.deleteTable(tblName);
        System.out.println("[DONE]");
      }
    }
  }
}
