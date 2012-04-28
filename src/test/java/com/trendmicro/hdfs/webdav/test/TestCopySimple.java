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
import org.apache.jackrabbit.webdav.MultiStatus;
import org.apache.jackrabbit.webdav.MultiStatusResponse;
import org.apache.jackrabbit.webdav.client.methods.CopyMethod;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCopySimple {

  private static final Log LOG = LogFactory.getLog(TestCopySimple.class);

  private static final String testData = "This is a test!\r\n";

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
    
    FsPermission.setUMask(conf, new FsPermission((short)0));

    FileSystem fs = minicluster.getTestFileSystem();
    Path path = new Path("/test");
    assertTrue(fs.mkdirs(path,
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)));
    fs.setOwner(path, ownerUser.getShortUserName(),
      ownerUser.getGroupNames()[0]);

    ownerUser.doAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        FileSystem fs = minicluster.getTestFileSystem();
        assertTrue(fs.mkdirs(new Path("/test/rw"),
          new FsPermission(FsAction.ALL, FsAction.WRITE_EXECUTE,
            FsAction.NONE)));
        assertTrue(fs.mkdirs(new Path("/test/ro"),
          new FsPermission(FsAction.READ_EXECUTE, FsAction.NONE,
            FsAction.NONE)));
        assertTrue(fs.mkdirs(new Path("/test/public"),
          new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)));
        FSDataOutputStream os = fs.create(new Path("/test/rw/file1"),
          new FsPermission(FsAction.ALL, FsAction.READ, FsAction.NONE),
          true, 4096, (short)1, 65536, null);
        assertNotNull(os);
        os.write(testData.getBytes());
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
  public void testCopyOwner() {
    // Copy the file
    CopyMethod copy = new CopyMethod("http://localhost:" +
        minicluster.getGatewayPort() + "/test/rw/file1?user.name=" +
        ownerUser.getShortUserName(),
      "http://localhost:" + minicluster.getGatewayPort() +
        "/test/rw/file2",
      true);
    try {
      int code = minicluster.getClient().executeMethod(copy);
      assertEquals("Expected 201 response, got " + code, 201, code);
    } catch (IOException e) {
      LOG.error("Copy failed", e);
      fail("Copy failed with an exception");
    } finally {
      copy.releaseConnection();
    }
    // Check the copy
    GetMethod get = new GetMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/rw/file2?user.name=" +
      ownerUser.getShortUserName());
    try {
      int code = minicluster.getClient().executeMethod(get);
      assertEquals("Expected 200 response, got " + code, 200, code);
      String data = get.getResponseBodyAsString();
      assertEquals("Response body was not as expected, wanted '" +
        testData + "', got '" + data + "'", testData, data);
    } catch (IOException e) {
      LOG.error("Get failed", e);
      fail("Get failed with an exception");      
    } finally {
      get.releaseConnection();
    }
  }

  @Test
  public void testCopyOwnerConflict() {
    CopyMethod copy1 = new CopyMethod("http://localhost:" +
        minicluster.getGatewayPort() + "/test/rw/file1?user.name=" +
        ownerUser.getShortUserName(),
      "http://localhost:" + minicluster.getGatewayPort() +
        "/test/rw/file3",
      true);
    try {
      int code = minicluster.getClient().executeMethod(copy1);
      assertEquals("Expected 201 response, got " + code, 201, code);
    } catch (IOException e) {
      LOG.error("Copy failed", e);
      fail("Copy failed with an exception");
    } finally {
      copy1.releaseConnection();
    }

    // Try it again and disallow overwriting
    CopyMethod copy2 = new CopyMethod("http://localhost:" +
        minicluster.getGatewayPort() + "/test/rw/file1?user.name=" +
        ownerUser.getShortUserName(),
      "http://localhost:" + minicluster.getGatewayPort() +
        "/test/rw/file3",
      false);
    try {
      int code = minicluster.getClient().executeMethod(copy2);
      assertEquals("Expected 412 response, got " + code, 412, code);
    } catch (IOException e) {
      LOG.error("Copy failed", e);
      fail("Copy failed with an exception");
    } finally {
      copy2.releaseConnection();
    }
  }

  @Test
  public void testCopyAnonymousPublic() {
    // Copy the file
    CopyMethod copy = new CopyMethod("http://localhost:" +
        minicluster.getGatewayPort() + "/test/rw/file1",
      "http://localhost:" + minicluster.getGatewayPort() +
        "/test/public/file4",
      true);
    try {
      int code = minicluster.getClient().executeMethod(copy);
      assertEquals("Expected 201 response, got " + code, 201, code);
    } catch (IOException e) {
      LOG.error("Copy failed", e);
      fail("Copy failed with an exception");
    } finally {
      copy.releaseConnection();
    }
    // Check the copy
    GetMethod get = new GetMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/public/file4");
    try {
      int code = minicluster.getClient().executeMethod(get);
      assertEquals("Expected 200 response, got " + code, 200, code);
      String data = get.getResponseBodyAsString();
      assertEquals("Response body was not as expected, wanted '" +
        testData + "', got '" + data + "'", testData, data);
    } catch (IOException e) {
      LOG.error("Get failed", e);
      fail("Get failed with an exception");      
    } finally {
      get.releaseConnection();
    }
  }

  @Test
  public void testCopyAnonymousReadOnly() {
    CopyMethod copy = new CopyMethod("http://localhost:" +
        minicluster.getGatewayPort() + "/test/rw/file1",
      "http://localhost:" + minicluster.getGatewayPort() +
        "/test/ro/file5",
      true);
    try {
      try {
        int code = minicluster.getClient().executeMethod(copy);
        assertEquals("Expected 207 response, got " + code, 207, code);      
      } catch (IOException e) {
        LOG.error("Copy failed", e);
        fail("Copy failed with an exception");
      }
      try {
        MultiStatus status = copy.getResponseBodyAsMultiStatus();
        MultiStatusResponse firstResponse =
          status.getResponses()[0];
        int code = firstResponse.getStatus()[0].getStatusCode();
        assertEquals(
          "Expected 401 status code in first multistatus response, got " +
          code, 401, code);      
      } catch (Exception e) {
        LOG.error("Multistatus parse failed", e);
        fail("Unable to parse multistatus response");
      }
    } finally {
      copy.releaseConnection();
    }
  }

}
