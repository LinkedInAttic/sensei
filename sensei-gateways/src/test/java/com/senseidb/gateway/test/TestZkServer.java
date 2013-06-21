package com.senseidb.gateway.test;


import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.Test;


/**
 * @author Dmytro Ivchenko
 */
public class TestZkServer
{
  private static ZooKeeperServer zkServer;
  private static NIOServerCnxn.Factory cnxnFactory;
  private static File zkDir;

  public void start() throws Exception
  {
    zkServer = new ZooKeeperServer();

    zkDir = File.createTempFile("zkData", Long.toString(System.nanoTime()));
    zkDir.delete();
    zkDir.mkdir();

    FileTxnSnapLog ftxn = new FileTxnSnapLog(zkDir, zkDir);
    zkServer.setTxnLogFactory(ftxn);
    zkServer.setTickTime(2000);
    zkServer.setMinSessionTimeout(-1);
    zkServer.setMaxSessionTimeout(-1);

    cnxnFactory = new NIOServerCnxn.Factory(new InetSocketAddress(InetAddress.getByName(
        "localhost"), 2181), 50);
    cnxnFactory.setDaemon(true);
    cnxnFactory.startup(zkServer);
  }

  @Test
  public void test()
  {

  }

  public void stop() {
    try {cnxnFactory.shutdown();} catch (Exception t){}
    try {zkServer.shutdown();} catch (Exception t){}
    rmrf(zkDir);
  }

  public static boolean rmrf(File f) {
    if (f == null || !f.exists()) {
      return true;
    }
    if (f.isDirectory()) {
      for (File sub : f.listFiles()) {
        if (!rmrf(sub))
          return false;
      }
    }
    return f.delete();
  }
}
