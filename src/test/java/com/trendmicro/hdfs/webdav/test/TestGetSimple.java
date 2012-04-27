package com.trendmicro.hdfs.webdav.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGetSimple {

  private static final Log LOG = LogFactory.getLog(TestGetSimple.class);

  private static final String testPublicData =
    "This is some read only public data.\r\n";

  private static final String testPrivateData =
    "This is some private data.\r\n";

  private static MiniClusterTestUtil minicluster = new MiniClusterTestUtil();
  private static UserGroupInformation ownerUser = 
    UserGroupInformation.createUserForTesting("owner",
      new String[] { "users" });
  private static UserGroupInformation gatewayUser = 
    UserGroupInformation.createUserForTesting("gateway",
      new String[] { "users" });

  @BeforeClass
  public static void setup() throws Exception {
    Configuration conf = minicluster.getConfiguration();
    conf.set("hadoop.proxyuser." +
      UserGroupInformation.getCurrentUser().getShortUserName() + ".groups",
        "users");
    conf.set("hadoop.proxyuser." +
      UserGroupInformation.getCurrentUser().getShortUserName() + ".hosts",
        "localhost");
    conf.set("hadoop.webdav.authentication.type", "simple");
    conf.setBoolean("hadoop.webdav.authentication.simple.anonymous.allowed",
      true);

    minicluster.startMiniCluster(gatewayUser);
    LOG.info("Gateway started on port " + minicluster.getGatewayPort());

    UserGroupInformation.getCurrentUser().doAs(
      new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          FileSystem fs = minicluster.getTestFileSystem();
          Path path = new Path("/test");
          fs.mkdirs(path,
            new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE,
              FsAction.NONE));
          fs.setOwner(path, ownerUser.getShortUserName(),
            ownerUser.getGroupNames()[0]);
          return null;
        }
      });

    ownerUser.doAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        FileSystem fs = minicluster.getTestFileSystem();
        FSDataOutputStream os;
        os = fs.create(new Path("/test/pubdata"),
            new FsPermission(FsAction.ALL, FsAction.READ, FsAction.NONE),
            true, 4096, (short)1, 65536, null);
        os.write(testPublicData.getBytes());
        os.close();
        os = fs.create(new Path("/test/privdata"),
            new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE),
            true, 4096, (short)1, 65536, null);
        os.write(testPrivateData.getBytes());
        os.close();
        return null;
      }
    });
  }

  @AfterClass
  public static void cleanup() {
    minicluster.shutdownMiniCluster();
  }

  @Test
  public void testGetAnonymousPublic() {
    try {
      GetMethod get = new GetMethod("http://localhost:" +
        minicluster.getGatewayPort() + "/test/pubdata");
      int code = minicluster.getClient().executeMethod(get);
      assertEquals("Expected 200 response, got " + code, 200, code);
      String data = get.getResponseBodyAsString();
      assertEquals("Response body was not as expected, wanted '" +
        testPublicData + "', got '" + data + "'", testPublicData, data);
    } catch (IOException e) {
      LOG.error("Get failed", e);
      fail("Get failed with an exception");
    }
  }

  @Test
  public void testGetAnonymousPrivate() {
    try {
      GetMethod get = new GetMethod("http://localhost:" +
        minicluster.getGatewayPort() + "/test/privdata");
      int code = minicluster.getClient().executeMethod(get);
      assertEquals("Expected 401 response, got " + code, 401, code);
    } catch (IOException e) {
      LOG.error("Get failed", e);
      fail("Get failed with an exception");
    }
  }

  @Test
  public void testGetOwnerPrivate() {
    try {
      GetMethod get = new GetMethod("http://localhost:" +
        minicluster.getGatewayPort() + "/test/privdata?user.name=" +
        ownerUser.getShortUserName());
      int code = minicluster.getClient().executeMethod(get);
      assertEquals("Expected 200 response, got " + code, 200, code);
      String data = get.getResponseBodyAsString();
      assertEquals("Response body was not as expected, wanted '" +
        testPrivateData + "', got '" + data + "'", testPrivateData, data);
    } catch (IOException e) {
      LOG.error("Get failed", e);
      fail("Get failed with an exception");
    }
  }

}
