package com.sampullara.pingcap;

import msgpb.Msgpb;
import pdpb.Pdpb;

import java.io.*;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by sam on 2/6/17.
 */
public class RPC {
  private final OutputStream os;
  private final InputStream is;

  private static LongAdder messageId = new LongAdder();

  static {
    messageId.add(new SecureRandom().nextLong());
  }

  private static ThreadLocal<ByteArrayOutputStream> cachedBaos = new ThreadLocal<ByteArrayOutputStream>() {
    @Override
    protected ByteArrayOutputStream initialValue() {
      return new ByteArrayOutputStream();
    }

    @Override
    public ByteArrayOutputStream get() {
      ByteArrayOutputStream baos = super.get();
      baos.reset();
      return baos;
    }
  };

  public enum RPCType {
    PD, KV
  }

  public RPC(String host, int port, RPCType type) throws TiException {
    try {
      Socket socket = new Socket(host, port);
      os = new BufferedOutputStream(socket.getOutputStream());
      is = new BufferedInputStream(socket.getInputStream());

      switch (type) {
        case PD:
          os.write(("GET /" + type + "/rpc HTTP/1.0\r\n\r\n").getBytes());
          os.flush();
          break;
        default:
      }
    } catch (IOException e) {
      throw new TiException("Could not connect to PD", e);
    }
  }

  public Pdpb.Response send(Pdpb.Request request) throws IOException {
    return send(Msgpb.Message.newBuilder()
            .setMsgType(Msgpb.MessageType.PdReq)
            .setPdReq(request).build())
            .getPdResp();
  }

  public Msgpb.Message send(Msgpb.Message message) throws IOException {
    // Write the message
    ByteArrayOutputStream baos = cachedBaos.get();
    message.writeTo(baos);
    writeHeader(os, baos.size());
    baos.writeTo(os);
    os.flush();

    // Get the response — later this needs to be async
    byte[] responseBytes = readHeader(is);
    return Msgpb.Message.parseFrom(responseBytes);
  }

  private static void writeHeader(OutputStream os, int length) throws IOException {
    DataOutputStream dos = new DataOutputStream(os);
    // Magic value
    dos.write(new byte[]{(byte) 0xda, (byte) 0xf4});
    // Version
    dos.writeShort(1);
    // Message length
    dos.writeInt(length);
    // Message Id
    dos.writeLong(messageId.longValue());
    // Go to the next message id
    messageId.increment();
  }

  private static byte[] readHeader(InputStream is) throws IOException {
    DataInputStream dis = new DataInputStream(is);
    int magic = dis.readShort();
    int version = dis.readShort();
    int length = dis.readInt();
    long id = dis.readLong();
    byte[] bytes = new byte[length];
    dis.readFully(bytes);
    return bytes;
  }

}
