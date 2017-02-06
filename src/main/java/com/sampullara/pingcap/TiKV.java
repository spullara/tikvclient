package com.sampullara.pingcap;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Created by sam on 2/6/17.
 */
public class TiKV implements DB {

  private final RPC pdRPC;
  private String[] pds;

  public TiKV(String... pds) {
    // we will use this later when we fail to talk to the PD
    this.pds = pds;
    // try and connect right away to ensure that we can
    pdRPC = getRpc(pds);
  }

  // Get a random connection to one of the PDs provided
  private RPC getRpc(String[] pds) {
    List<String> list = Arrays.asList(pds);
    Collections.shuffle(list);
    return list.stream()
            // format of the string is host:port
            .map(pd -> pd.split(":"))
            // try and connect
            .map(address -> {
              try {
                return new RPC(address[0], Integer.parseInt(address[1]), RPC.RPCType.PD);
              } catch (TiException tie) {
                return null;
              }
            })
            // if it connected then
            .filter(Objects::nonNull)
            // return it
            .findFirst()
            // if none connect then complain
            .orElseGet(() -> {
              throw new TiException("No PDs are reachable: " + Arrays.toString(pds));
            });
  }

  @Override
  public <T> T run(Function<Tx, T> f) {
    Tx tx = new TiTx(this);
    return f.apply(tx);
  }
}
