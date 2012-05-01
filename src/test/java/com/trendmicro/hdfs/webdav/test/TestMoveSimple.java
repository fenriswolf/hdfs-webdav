/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.jackrabbit.webdav.client.methods.MoveMethod;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMoveSimple {
  private static final Log LOG = LogFactory.getLog(TestMoveSimple.class);

  private static final String testData = "This is a move test!\r\n";

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
        assertTrue(fs.mkdirs(new Path("/test/owner"),
          new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE,
            FsAction.NONE)));
        assertTrue(fs.mkdirs(new Path("/test/public"),
          new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)));
        FSDataOutputStream os = fs.create(new Path("/test/owner/file1"),
          new FsPermission(FsAction.ALL, FsAction.READ, FsAction.NONE),
          true, 4096, (short)1, 65536, null);
        assertNotNull(os);
        os.write(testData.getBytes());
        os.close();
        os = fs.create(new Path("/test/public/file1"),
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
  public void testMoveOwner() {
    // Copy the file
    MoveMethod move = new MoveMethod("http://localhost:" +
        minicluster.getGatewayPort() + "/test/owner/file1?user.name=" +
        ownerUser.getShortUserName(),
      "http://localhost:" + minicluster.getGatewayPort() + "/test/owner/file2",
      true);
    try {
      int code = minicluster.getClient().executeMethod(move);
      assertEquals("Expected 201 response, got " + code, 201, code);
    } catch (IOException e) {
      LOG.error("Move failed", e);
      fail("Move failed with an exception");
    } finally {
      move.releaseConnection();
    }
    // Check the file was moved
    GetMethod get = new GetMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/owner/file1?user.name=" +
      ownerUser.getShortUserName());
    try {
      int code = minicluster.getClient().executeMethod(get);
      assertEquals("Expected 404 response, got " + code, 404, code);
    } catch (IOException e) {
      LOG.error("Get failed", e);
      fail("Get failed with an exception");      
    } finally {
      get.releaseConnection();
    }
    // Check the target
    get = new GetMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/owner/file2?user.name=" +
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
  public void testMoveAnonymousPublic() {
    MoveMethod move = new MoveMethod("http://localhost:" +
        minicluster.getGatewayPort() + "/test/public/file1",
      "http://localhost:" + minicluster.getGatewayPort() +
        "/test/public/file2",
      true);
    try {
      int code = minicluster.getClient().executeMethod(move);
      assertEquals("Expected 201 response, got " + code, 201, code);
    } catch (IOException e) {
      LOG.error("Move failed", e);
      fail("Move failed with an exception");
    } finally {
      move.releaseConnection();
    }
    // Check that the source no longer exists
    GetMethod get = new GetMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/public/file1");
    try {
      int code = minicluster.getClient().executeMethod(get);
      assertEquals("Expected 404 response, got " + code, 404, code);
    } catch (IOException e) {
      LOG.error("Get failed", e);
      fail("Get failed with an exception");      
    } finally {
      get.releaseConnection();
    }
    // Check the target
    get = new GetMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/public/file2");
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

}
