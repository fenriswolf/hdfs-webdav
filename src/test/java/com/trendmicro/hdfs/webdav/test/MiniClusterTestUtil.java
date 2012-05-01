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

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.UUID;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;

import com.trendmicro.hdfs.webdav.AuthFilter;
import com.trendmicro.hdfs.webdav.HDFSWebDAVServlet;

public class MiniClusterTestUtil {

  public static final String TEST_DIRECTORY_KEY = "test.build.data";
  public static final String DEFAULT_TEST_DIRECTORY = "target/test-data";
  public static final int DEFAULT_GATEWAY_PORT = 8080;

  private static final Log LOG = LogFactory.getLog(MiniClusterTestUtil.class);

  private MiniDFSCluster dfsCluster;
  private File clusterTestBuildDir;

  private HDFSWebDAVServlet servlet;
  private Server servletServer;
  private Context servletContext;
  private int gatewayPort;

  public HDFSWebDAVServlet getServlet() {
    if (servlet == null) {
      servlet = HDFSWebDAVServlet.getServlet();
    }
    return servlet;
  }

  public Configuration getConfiguration() {
    return getServlet().getConfiguration();
  }

  public static Path getTestDir() {
    return new Path(System.getProperty(TEST_DIRECTORY_KEY,
      DEFAULT_TEST_DIRECTORY));
  }

  public static Path getTestDir(final String subdirName) {
    return new Path(getTestDir(), subdirName);
  }

  public File setupClusterTestBuildDir() {
    String randomStr = UUID.randomUUID().toString();
    String dirStr = getTestDir(randomStr).toString();
    File dir = new File(dirStr).getAbsoluteFile();
    // Have it cleaned up on exit
    dir.deleteOnExit();
    return dir;
  }

  public void initTestDir() {
    if (System.getProperty(TEST_DIRECTORY_KEY) == null) {
      clusterTestBuildDir = setupClusterTestBuildDir();
      System.setProperty(TEST_DIRECTORY_KEY, clusterTestBuildDir.getPath());
    }
  }

  public FileSystem getTestFileSystem() throws IOException {
    return FileSystem.get(getConfiguration());
  }

  public boolean cleanupTestDir() throws IOException {
    return deleteDir(getTestDir());
  }

  public boolean cleanupTestDir(final String subdir) throws IOException {
    return deleteDir(getTestDir(subdir));
  }

  public boolean deleteDir(final Path dir) throws IOException {
    FileSystem fs = getTestFileSystem();
    if (fs.exists(dir)) {
      return fs.delete(getTestDir(), true);
    }
    return false;
  }

  public MiniDFSCluster startMiniDFSCluster(int servers) throws Exception {
    return startMiniDFSCluster(servers, null, null);
  }

  public MiniDFSCluster startMiniDFSCluster(final String hosts[])
      throws Exception {
    if (hosts != null && hosts.length != 0) {
      return startMiniDFSCluster(hosts.length, null, hosts);
    } else {
      return startMiniDFSCluster(1, null, null);
    }
  }

  public MiniDFSCluster startMiniDFSCluster(int servers, final File dir)
      throws Exception {
    return startMiniDFSCluster(servers, dir, null);
  }

  public MiniDFSCluster startMiniDFSCluster(int servers, final File dir,
      final String hosts[]) throws Exception {
    if (dir == null) {
      clusterTestBuildDir = setupClusterTestBuildDir();
    } else {
      clusterTestBuildDir = dir;
    }
    System.setProperty(TEST_DIRECTORY_KEY, clusterTestBuildDir.toString());
    System.setProperty("test.cache.data", clusterTestBuildDir.toString());
    Configuration conf = getConfiguration();
    dfsCluster = new MiniDFSCluster(0, conf, servers, true, true, true,
      null, null, hosts, null);
    FileSystem fs = dfsCluster.getFileSystem();
    conf.set("fs.defaultFS", fs.getUri().toString());
    conf.set("fs.default.name", fs.getUri().toString());
    return dfsCluster;
  }

  public void shutdownMiniDFSCluster() throws Exception {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  public MiniDFSCluster getDFSCluster() {
    return dfsCluster;
  }

  private void startServletServer(int port) throws Exception {
    if (servletServer != null) {
      throw new IOException("Servlet server already running");
    }

    servletServer = new Server();

    servletServer.setSendServerVersion(false);
    servletServer.setSendDateHeader(false);
    servletServer.setStopAtShutdown(true);

    SelectChannelConnector connector = new SelectChannelConnector();
    connector.setPort(port);
    servletServer.addConnector(connector);

    // Set up context
    servletContext = new Context(servletServer, "/", Context.SESSIONS);
    ServletHolder servletHolder = new ServletHolder(getServlet());
    servletHolder.setInitParameter("authenticate-header",
      "Basic realm=\"Hadoop WebDAV Server\"");
    servletContext.addServlet(servletHolder, "/*");
    servletContext.addFilter(
      new FilterHolder(new AuthFilter(getConfiguration())), "/*", 0);
    servletServer.setHandler(servletContext);

    servletServer.start();
  }

  public void shutdownServletServer() {
    if (servletServer != null) try {
      servletServer.stop();
    } catch (Exception e) {
      LOG.error("Failed to stop servlet server", e);
    }
    servletServer = null;
    servletContext = null;
  }

  public void startHDFSWebDAVServlet(UserGroupInformation gatewayUser)
      throws Exception {
    gatewayPort = getConfiguration().getInt("hadoop.webdav.port",
      DEFAULT_GATEWAY_PORT);
    while (true) try {
      gatewayUser.doAs(new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          startServletServer(gatewayPort);
          return null;
        }
      });
      break;
    } catch (Exception e) {
      LOG.info("Unable to start Jetty on port " + gatewayPort, e);
      gatewayPort++;
    }
    getConfiguration().setInt("hadoop.webdav.port", gatewayPort);
  }

  public void shutdownHDFSWebDAVServlet() {
    shutdownServletServer();
  }

  public Server getServletServer() {
    return servletServer;
  }

  public Context getServletContext() {
    return servletContext;
  }

  public int getGatewayPort() {
    return gatewayPort;
  }
  
  public void startMiniCluster() throws Exception {
    startMiniCluster(UserGroupInformation.createUserForTesting("gateway",
      new String[] { "users" }));
  }

  public void startMiniCluster(UserGroupInformation gatewayUser)
      throws Exception {
    for (Logger log: new Logger[] { 
        Logger.getLogger("com.trendmicro"),
        Logger.getLogger("org.mortbay") }) {
      log.setLevel(Level.DEBUG);
    }
    try {
      startMiniDFSCluster(1);
    } catch (Exception e) {
      LOG.error("Failed to start DFS cluster", e);
      throw e;
    }
    try {
      startHDFSWebDAVServlet(gatewayUser);
    } catch (Exception e) {
      LOG.error("Failed to start servlet", e);
      try {
        shutdownMiniDFSCluster();
      } catch (Exception ex) {
        LOG.error("Failed to shut down DFS cluster", ex);
      }
      throw e;
    }
  }

  public void shutdownMiniCluster() {
    shutdownHDFSWebDAVServlet();
    try {
      shutdownMiniDFSCluster();
    } catch (Exception e) {
      LOG.error("Failed to shut down DFS cluster", e);
    }
  }

  public HttpClient getClient() {
    HostConfiguration hostConfig = new HostConfiguration();
    hostConfig.setHost("localhost"); 
    HttpConnectionManager connectionManager =
      new MultiThreadedHttpConnectionManager();
    HttpConnectionManagerParams params = new HttpConnectionManagerParams();
    params.setMaxConnectionsPerHost(hostConfig, 100);
    connectionManager.setParams(params);   
    HttpClient client = new HttpClient(connectionManager);
    client.setHostConfiguration(hostConfig);
    return client;
  }

}
