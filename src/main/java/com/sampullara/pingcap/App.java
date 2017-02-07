package com.sampullara.pingcap;

import java.io.IOException;
import java.util.UUID;

/**
 * Hello world!
 */
public class App {

  public static void main(String[] args) throws IOException {
    String expected = UUID.randomUUID().toString();

    TiKV tiKV = new TiKV("localhost:2379", "127.0.0.1:2379");
    tiKV.set("foo".getBytes(), expected.getBytes());
    String actual = new String(tiKV.get("foo".getBytes()));

    System.out.println(actual + " == " + expected + ": " + actual.equals(expected));
  }

}
