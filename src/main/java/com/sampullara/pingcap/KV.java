package com.sampullara.pingcap;

/**
 * Created by sam on 2/6/17.
 */
public class KV {
  public final byte[] key;
  public final byte[] value;

  public KV(byte[] key, byte[] value) {
    this.key = key;
    this.value = value;
  }
}
