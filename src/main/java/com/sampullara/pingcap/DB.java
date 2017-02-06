package com.sampullara.pingcap;

import java.util.function.Function;

/**
 * Created by sam on 2/6/17.
 */
public interface DB {
  <T> T run(Function<Tx, T> f);
}
