package com.sampullara.pingcap;

import java.util.function.Function;

/**
 * Created by sam on 2/6/17.
 */
public interface DB {
  // Transactional
  <T> T run(Function<Tx, T> f);

  // Non-transactional
  void set(byte[] key, byte[] value);
  byte[] get(byte[] key);
}
