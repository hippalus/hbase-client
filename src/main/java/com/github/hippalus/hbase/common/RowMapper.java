package com.github.hippalus.hbase.common;

import java.util.Optional;
import org.apache.hadoop.hbase.client.Result;

public interface RowMapper<T> {

  Optional<T> mapRow(Result result, int rowNum);
}
