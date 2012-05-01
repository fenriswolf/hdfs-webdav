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

package com.trendmicro.hdfs.webdav;

import java.io.IOException;
import java.net.MalformedURLException;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.jackrabbit.webdav.DavException;
import org.apache.jackrabbit.webdav.DavLocatorFactory;
import org.apache.jackrabbit.webdav.DavResource;
import org.apache.jackrabbit.webdav.DavResourceFactory;
import org.apache.jackrabbit.webdav.DavSessionProvider;
import org.apache.jackrabbit.webdav.MultiStatus;
import org.apache.jackrabbit.webdav.MultiStatusResponse;
import org.apache.jackrabbit.webdav.WebdavRequest;
import org.apache.jackrabbit.webdav.WebdavResponseImpl;
import org.apache.jackrabbit.webdav.server.AbstractWebdavServlet;
import org.apache.jackrabbit.webdav.simple.LocatorFactoryImpl;
import org.apache.jackrabbit.webdav.simple.ResourceConfig;
import org.apache.tika.detect.DefaultDetector;

public class HDFSWebDAVServlet extends AbstractWebdavServlet {

  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(HDFSWebDAVServlet.class);

  /**
   * init param name of the repository prefix
   */
  public static final String INIT_PARAM_RESOURCE_PATH_PREFIX =
    "resource-path-prefix";

  /**
   * Name of the init parameter that specify a separate configuration used
   * for filtering the resources displayed.
   */
  public static final String INIT_PARAM_RESOURCE_CONFIG = "resource-config";

  /**
  * Servlet context attribute used to store the path prefix instead of
  * having a static field with this servlet. The latter causes problems
  * when running multiple
  */
  public static final String CTX_ATTR_RESOURCE_PATH_PREFIX =
    "hadoop.webdav.resourcepath";

  private static HDFSWebDAVServlet instance;

  public static synchronized HDFSWebDAVServlet getServlet() {
    if (instance == null) {
      instance = new HDFSWebDAVServlet();
    }
    return instance;
  }

  private String resourcePathPrefix;
  private DavResourceFactory resourceFactory;
  private DavLocatorFactory locatorFactory;
  private DavSessionProvider sessionProvider;
  private ResourceConfig resourceConf;
  private Configuration hadoopConf;

  protected HDFSWebDAVServlet() { }

  public synchronized Configuration getConfiguration() {
    if (hadoopConf == null) {
      hadoopConf = new Configuration();
      hadoopConf.addResource("webdav-default.xml");
      hadoopConf.addResource("webdav-site.xml");
    }
    return hadoopConf;
  }

  private synchronized ResourceConfig getResourceConfig() {
    if (resourceConf == null) {
      resourceConf = new ResourceConfig(new DefaultDetector());
    }
    return resourceConf;
  }

  @Override
  protected boolean isPreconditionValid(WebdavRequest request,
      DavResource resource) {
    try {
      HDFSResource dfsResource = (HDFSResource)resource;
      dfsResource.setProxyUser(request.getRemoteUser());
      return !resource.exists() || request.matchesIfHeader(resource);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected int validateDestination(DavResource resource, WebdavRequest request,
      boolean overwrite) throws DavException {
    try {
      HDFSResource dfsResource = (HDFSResource)resource;
      dfsResource.setProxyUser(request.getRemoteUser());
      return super.validateDestination(dfsResource, request, overwrite);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DavLocatorFactory getLocatorFactory() {
    if (locatorFactory == null) {
      locatorFactory = new LocatorFactoryImpl(resourcePathPrefix);
    }
    return locatorFactory;
  }

  @Override
  public void setLocatorFactory(DavLocatorFactory locatorFactory) {
    this.locatorFactory = locatorFactory; 
  }

  @Override
  public DavResourceFactory getResourceFactory() {
    if (resourceFactory == null) {
      resourceFactory = new HDFSResourceFactory(getResourceConfig(),
        getConfiguration());
    }
    return resourceFactory;
  }

  @Override
  public void setResourceFactory(DavResourceFactory resourceFactory) {
    this.resourceFactory = resourceFactory;
  }

  @Override
  public synchronized DavSessionProvider getDavSessionProvider() {
    if (sessionProvider == null) {
      sessionProvider = new SessionProvider();
    }
    return sessionProvider;
  }

  @Override
  public void setDavSessionProvider(DavSessionProvider sessionProvider) {
    this.sessionProvider = sessionProvider;
  }

  @Override
  public void init() throws ServletException {
    super.init();
    resourcePathPrefix = getInitParameter(INIT_PARAM_RESOURCE_PATH_PREFIX);
    if (resourcePathPrefix == null) {
      LOG.debug("Missing path prefix -> setting to empty string.");
      resourcePathPrefix = "";
    } else if (resourcePathPrefix.endsWith("/")) {
      LOG.debug("Path prefix ends with '/' -> removing trailing slash.");
      resourcePathPrefix =
        resourcePathPrefix.substring(0, resourcePathPrefix.length() - 1);
    }

    LOG.info("ServletName: " + getServletName());
    LOG.info("ServletInfo: " + getServletInfo());

    ServletContext context = getServletContext();

    context.setAttribute(CTX_ATTR_RESOURCE_PATH_PREFIX, resourcePathPrefix);
    LOG.info(INIT_PARAM_RESOURCE_PATH_PREFIX + " is '" + resourcePathPrefix + "'");

    String configParam = getInitParameter(INIT_PARAM_RESOURCE_CONFIG);
    if (configParam != null) try {
      getResourceConfig().parse(context.getResource(configParam));
    } catch (MalformedURLException e) {
      LOG.debug("Unable to build resource filter provider");
    }
  }

  @Override
  protected void service(HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {  
    if (LOG.isDebugEnabled()) {
      LOG.debug(request.getMethod() + " for '" + request.getRequestURI() +
        "' from " + request.getRemoteUser() + " at " + request.getRemoteAddr());
    }
    try {
      super.service(request, response);
    } catch (Exception e) {
      if (e instanceof AccessControlException || 
          e.getCause() instanceof AccessControlException) {
        LOG.info("Insufficient permissions for request for '" +
          request.getRequestURI() + "' from " + request.getRemoteUser() +
          " at " + request.getRemoteAddr());
        if (request.getMethod().equalsIgnoreCase("GET")) {
          // Provide a plain 401 response for GETs
          new WebdavResponseImpl(response)
            .sendError(HttpServletResponse.SC_UNAUTHORIZED);
        } else {
          // Otherwise send a multistatus response
          MultiStatus ms = new MultiStatus();
          ms.addResponse(
            new MultiStatusResponse(request.getRequestURL().toString(), 401,
              "You do not have permission to access this resource."));
          new WebdavResponseImpl(response).sendMultiStatus(ms);
        }
      } else {
        LOG.warn("Exception processing request for '" +
          request.getRequestURI() + "' from " + request.getRemoteUser() +
          " at " + request.getRemoteAddr() + " authType " +
          request.getAuthType(), e);
        new WebdavResponseImpl(response)
          .sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    }
  }

}