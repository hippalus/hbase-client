package com.github.hippalus.hbase.testutil.cluster;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Connection;

public interface HBaseCluster {

  Connection start() throws IOException;

  void end() throws Exception;


}
