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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.jackrabbit.webdav.MultiStatus;
import org.apache.jackrabbit.webdav.MultiStatusResponse;
import org.apache.jackrabbit.webdav.client.methods.PutMethod;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestPutSimple {

  private static final Log LOG = LogFactory.getLog(TestPutSimple.class);

  private static final String testData = "This is a put test!";

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
        return null;
      }
    });
  }

  @AfterClass
  public static void cleanup() {
    minicluster.shutdownMiniCluster();
  }

  @Test
  public void testPutOwner() {
    PutMethod put = new PutMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/rw/file1?user.name=" +
      ownerUser.getShortUserName());
    put.setRequestBody(testData);
    try {
      int code = minicluster.getClient().executeMethod(put);
      assertEquals("Expected 201 response, got " + code, 201, code);
    } catch (IOException e) {
      LOG.error("Put failed", e);
      fail("Put failed with an exception");
    } finally {
      put.releaseConnection();
    }
    // Check the result
    GetMethod get = new GetMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/rw/file1?user.name=" +
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
  public void testPutOwnerReadOnly() {
    PutMethod put = new PutMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/ro/file2?user.name=" +
      ownerUser.getShortUserName());
    put.setRequestBody(testData);
    try {
      try {
        int code = minicluster.getClient().executeMethod(put);
        assertEquals("Expected 207 response, got " + code, 207, code);      
      } catch (IOException e) {
        LOG.error("Put failed", e);
        fail("Put failed with an exception");
      }
      try {
        MultiStatus status = put.getResponseBodyAsMultiStatus();
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
      put.releaseConnection();
    }
  }

  @Test
  public void testPutAnonymousPublic() {
    PutMethod put = new PutMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/public/file3");
    put.setRequestBody(testData);
    try {
      int code = minicluster.getClient().executeMethod(put);
      assertEquals("Expected 201 response, got " + code, 201, code);
    } catch (IOException e) {
      LOG.error("Put failed", e);
      fail("Put failed with an exception");
    } finally {
      put.releaseConnection();
    }    
    // Check the result
    GetMethod get = new GetMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/public/file3");
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
  public void testPutAnonymousReadOnly() {
    PutMethod put = new PutMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/ro/file4");
    put.setRequestBody(testData);
    try {
      try {
        int code = minicluster.getClient().executeMethod(put);
        assertEquals("Expected 207 response, got " + code, 207, code);      
      } catch (IOException e) {
        LOG.error("Put failed", e);
        fail("Put failed with an exception");
      }
      try {
        MultiStatus status = put.getResponseBodyAsMultiStatus();
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
      put.releaseConnection();
    }
  }

}
