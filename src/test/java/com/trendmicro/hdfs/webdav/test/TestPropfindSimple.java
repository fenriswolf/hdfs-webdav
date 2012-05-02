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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.jackrabbit.webdav.DavConstants;
import org.apache.jackrabbit.webdav.DavException;
import org.apache.jackrabbit.webdav.MultiStatus;
import org.apache.jackrabbit.webdav.MultiStatusResponse;
import org.apache.jackrabbit.webdav.client.methods.PropFindMethod;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPropfindSimple {
  private static final Log LOG = LogFactory.getLog(TestPropfindSimple.class);

  private static final String testPublicData = "This is some public test data!\r\n";
  private static final String testPrivateData = "This is some privatetest data!\r\n";

  private static MiniClusterTestUtil minicluster = new MiniClusterTestUtil();
  private static UserGroupInformation ownerUser = 
    UserGroupInformation.createUserForTesting("owner",
      new String[] { "users" });
  private static UserGroupInformation gatewayUser = 
    UserGroupInformation.createUserForTesting("gateway",
      new String[] { "users" });

  private static final Path[] publicDirPaths = {
    new Path("/test/public"),
    new Path("/test/public/dir1"), new Path("/test/public/dir2"),
    new Path("/test/public/dir3")
  };

  private static final Path[] privateDirPaths = {
    new Path("/test/private"),
    new Path("/test/private/dir4"), new Path("/test/private/dir5"),
    new Path("/test/private/dir6")
  };

  private static final Path[] publicFilePaths = {
    new Path("/test/public/file1")
  };

  private static final Path[] privateFilePaths = {
    new Path("/test/private/file2")
  };

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
        for (Path dir: publicDirPaths) {
          assertTrue(fs.mkdirs(dir, new FsPermission(FsAction.ALL,
            FsAction.READ_EXECUTE, FsAction.NONE)));
        }
        for (Path dir: privateDirPaths) {
          assertTrue(fs.mkdirs(dir, new FsPermission(FsAction.ALL,
            FsAction.NONE, FsAction.NONE)));
        }
        for (Path path: publicFilePaths) {
          FSDataOutputStream os = fs.create(path,
            new FsPermission(FsAction.ALL, FsAction.READ, FsAction.NONE),
            true, 4096, (short)1, 65536, null);
          assertNotNull(os);
          os.write(testPublicData.getBytes());
          os.close();
        }
        for (Path path: privateFilePaths) {
          FSDataOutputStream os = fs.create(path,
            new FsPermission(FsAction.ALL, FsAction.READ, FsAction.NONE),
            true, 4096, (short)1, 65536, null);
          assertNotNull(os);
          os.write(testPrivateData.getBytes());
          os.close();
        }
        return null;
      }
    });

  }

  @AfterClass
  public static void cleanup() {
    minicluster.shutdownMiniCluster();
  }

  private List<String> propfindDirEnum(String path, String user)
      throws IOException, DavException {
    if (user != null) {
      user = "?user.name=" + user;
    } else {
      user = "";
    }
    PropFindMethod pfind = new PropFindMethod("http://localhost:" +
        minicluster.getGatewayPort() + path + user,
      DavConstants.PROPFIND_ALL_PROP, DavConstants.DEPTH_1);
    try {
      int code = minicluster.getClient().executeMethod(pfind);
      assertEquals("Expected 207 response, got " + code, 207, code);
      MultiStatus status = pfind.getResponseBodyAsMultiStatus();
      MultiStatusResponse[] responses = status.getResponses();
      List<String> files = new ArrayList<String>();
      for (MultiStatusResponse response: responses) {
        files.add(response.getHref());
      }
      return files;
    } finally {
      pfind.releaseConnection();
    }
  }

  @Test
  public void testPropfindDirEnumOwner() throws Exception {
    List<String> paths = propfindDirEnum("/test/private",
      ownerUser.getShortUserName());
    for (Path path: privateDirPaths) {
      assertTrue(path.toString() + " missing from propfind results",
        paths.contains(path.toString()));
    }
    for (Path path: privateFilePaths) {
      assertTrue(path.toString() + " missing from propfind results",
        paths.contains(path.toString()));
    }
  }

  @Test
  public void testPropfindDirEnumAnonymousPublic() throws Exception {
    List<String> paths = propfindDirEnum("/test/public", null);
    for (Path path: publicDirPaths) {
      assertTrue(path.toString() + " missing from propfind results",
        paths.contains(path.toString()));
    }
    for (Path path: publicFilePaths) {
      assertTrue(path.toString() + " missing from propfind results",
        paths.contains(path.toString()));
    }
  }

  @Test
  public void testPropfindDirEnumAnonymousPrivate() throws Exception {
    PropFindMethod pfind = new PropFindMethod("http://localhost:" +
        minicluster.getGatewayPort() + "/test/private",
      DavConstants.PROPFIND_ALL_PROP, DavConstants.DEPTH_1);
    try {
      try {
        int code = minicluster.getClient().executeMethod(pfind);
        assertEquals("Expected 207 response, got " + code, 207, code);
      } catch (IOException e) {
        LOG.error("Propfind failed", e);
        fail("Propfind failed with an exception");
      }
      try {
        MultiStatus status = pfind.getResponseBodyAsMultiStatus();
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
      pfind.releaseConnection();
    }
  }

}