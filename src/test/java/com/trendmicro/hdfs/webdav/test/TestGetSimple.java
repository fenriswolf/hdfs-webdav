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
        FSDataOutputStream os;
        os = fs.create(new Path("/test/pubdata"),
          new FsPermission(FsAction.ALL, FsAction.READ, FsAction.NONE),
          true, 4096, (short)1, 65536, null);
        assertNotNull(os);
        os.write(testPublicData.getBytes());
        os.close();
        os = fs.create(new Path("/test/privdata"),
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE),
          true, 4096, (short)1, 65536, null);
        assertNotNull(os);
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
    GetMethod get = new GetMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/pubdata");
    try {
      int code = minicluster.getClient().executeMethod(get);
      assertEquals("Expected 200 response, got " + code, 200, code);
      String data = get.getResponseBodyAsString();
      assertEquals("Response body was not as expected, wanted '" +
        testPublicData + "', got '" + data + "'", testPublicData, data);
    } catch (IOException e) {
      LOG.error("Get failed", e);
      fail("Get failed with an exception");
    } finally {
      get.releaseConnection();
    }
  }

  @Test
  public void testGetAnonymousPrivate() {
    GetMethod get = new GetMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/privdata");
    try {
      int code = minicluster.getClient().executeMethod(get);
      assertEquals("Expected 401 response, got " + code, 401, code);
    } catch (IOException e) {
      LOG.error("Get failed", e);
      fail("Get failed with an exception");
    } finally {
      get.releaseConnection();
    }
  }

  @Test
  public void testGetOwnerPrivate() {
    GetMethod get = new GetMethod("http://localhost:" +
      minicluster.getGatewayPort() + "/test/privdata?user.name=" +
      ownerUser.getShortUserName());
    try {
      int code = minicluster.getClient().executeMethod(get);
      assertEquals("Expected 200 response, got " + code, 200, code);
      String data = get.getResponseBodyAsString();
      assertEquals("Response body was not as expected, wanted '" +
        testPrivateData + "', got '" + data + "'", testPrivateData, data);
    } catch (IOException e) {
      LOG.error("Get failed", e);
      fail("Get failed with an exception");
    } finally {
      get.releaseConnection();
    }
  }

}
