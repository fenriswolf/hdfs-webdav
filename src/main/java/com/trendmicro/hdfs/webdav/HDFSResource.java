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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.jackrabbit.webdav.DavCompliance;
import org.apache.jackrabbit.webdav.DavConstants;
import org.apache.jackrabbit.webdav.DavException;
import org.apache.jackrabbit.webdav.DavResource;
import org.apache.jackrabbit.webdav.DavResourceFactory;
import org.apache.jackrabbit.webdav.DavResourceIterator;
import org.apache.jackrabbit.webdav.DavResourceIteratorImpl;
import org.apache.jackrabbit.webdav.DavResourceLocator;
import org.apache.jackrabbit.webdav.DavServletResponse;
import org.apache.jackrabbit.webdav.DavSession;
import org.apache.jackrabbit.webdav.MultiStatusResponse;
import org.apache.jackrabbit.webdav.io.InputContext;
import org.apache.jackrabbit.webdav.io.OutputContext;
import org.apache.jackrabbit.webdav.lock.ActiveLock;
import org.apache.jackrabbit.webdav.lock.LockInfo;
import org.apache.jackrabbit.webdav.lock.LockManager;
import org.apache.jackrabbit.webdav.lock.Scope;
import org.apache.jackrabbit.webdav.lock.SimpleLockManager;
import org.apache.jackrabbit.webdav.lock.Type;
import org.apache.jackrabbit.webdav.property.DavProperty;
import org.apache.jackrabbit.webdav.property.DavPropertyName;
import org.apache.jackrabbit.webdav.property.DavPropertySet;
import org.apache.jackrabbit.webdav.property.DefaultDavProperty;
import org.apache.jackrabbit.webdav.property.PropEntry;
import org.apache.jackrabbit.webdav.property.ResourceType;
import org.apache.jackrabbit.webdav.security.SecurityConstants;
import org.apache.jackrabbit.webdav.simple.ResourceConfig;

public class HDFSResource implements DavResource {

  private static final Log LOG = LogFactory.getLog(HDFSResource.class);

  private static final String COMPLIANCE_CLASS =
    DavCompliance.concatComplianceClasses(new String[] {DavCompliance._2_});

  // We support compliance level 1, and the listed methods
  private static final String SUPPORTED_METHODS =
    "OPTIONS, GET, HEAD, POST, TRACE, MKCOL, COPY, PUT, DELETE, MOVE, PROPFIND";

  private LockManager lockManager = new SimpleLockManager();
  private DavPropertySet properties;
  private DavResourceFactory factory;
  private DavResourceLocator locator;
  private DavSession session;
  private final Configuration conf;
  private final Path path; //the path object that this resource represents
  private boolean isCollectionRequest = false;
  private UserGroupInformation user;

  public HDFSResource(DavResourceFactory factory, DavResourceLocator locator,
      DavSession session, ResourceConfig resourceConf, Configuration conf)
      throws IOException {
    this(factory, locator, session, resourceConf, conf, false);
  }

  public HDFSResource(DavResourceFactory factory, DavResourceLocator locator,
      DavSession session, ResourceConfig resourceConf, Configuration conf,
      boolean isCollectionRequest)
      throws IOException {
    super();
    this.factory = factory;
    this.locator = locator;
    this.session = session;
    this.conf = conf;
    @SuppressWarnings("deprecation")
    String pathStr = URLDecoder.decode(locator.getResourcePath());
    if (pathStr.trim().equals("")) { //empty path is not allowed
        pathStr = "/";
    }
    this.path = new Path(pathStr);
    this.isCollectionRequest = isCollectionRequest;
  }

  private Path getPath() {
    return path;
  }

  public void setProxyUser(final String user) throws IOException {
    if (user != null) {
      this.user = UserGroupInformation.createProxyUser(user,
        UserGroupInformation.getLoginUser());
    }
    if (this.user == null) {
      this.user = UserGroupInformation.getCurrentUser();
    }
  }

  @Override
  public void addLockManager(final LockManager lockManager) {
    this.lockManager = lockManager;
  }

  @Override
  public void addMember(final DavResource resource, final InputContext context)
      throws DavException {
    // A PUT performed on an existing resource replaces the GET response entity
    // of the resource. Properties defined on the resource may be recomputed
    // during PUT processing but are not otherwise affected.
    final HDFSResource dfsResource = (HDFSResource)resource;
    final Path destPath = dfsResource.getPath();
    try {
      if (dfsResource.isCollectionRequest) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Creating new directory '" +
            destPath.toUri().getPath() + "'");
        }
        boolean success = user.doAs(new PrivilegedExceptionAction<Boolean>() {
          public Boolean run() throws Exception {
            return FileSystem.get(conf).mkdirs(destPath);
          }
        });
        if (!success) {
          throw new DavException(DavServletResponse.SC_CONFLICT);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Creating new file '" + destPath.toUri().getPath() + "'");
        }
        if (!context.hasStream() || context.getContentLength() < 0) {
          boolean success = user.doAs(new PrivilegedExceptionAction<Boolean>() {
            public Boolean run() throws Exception {
              return FileSystem.get(conf).createNewFile(destPath);
            }
          });
          if (!success) {
            throw new DavException(DavServletResponse.SC_CONFLICT);
          }
        } else {
          user.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
              OutputStream out = FileSystem.get(conf).create(destPath);
              InputStream in = context.getInputStream();
              IOUtils.copyBytes(in, out, conf, true);
              return null;
            }
          });
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public MultiStatusResponse alterProperties(final List<? extends PropEntry> props)
      throws DavException {
    return null;
  }

  @Override
  public void copy(final DavResource resource, final boolean shallow)
      throws DavException {
    if (!exists()) {
      throw new DavException(DavServletResponse.SC_NOT_FOUND);
    }
    if (!shallow || !isCollection()) {
      final HDFSResource dfsResource = (HDFSResource)resource;
      final Path destPath = dfsResource.getPath();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Copying '" + path.toUri().getPath() + "' to '" +
          destPath.toUri().getPath() + "'");
      }
      try {
        user.doAs(new PrivilegedExceptionAction<Void>() {
          public Void run() throws Exception {
            FileSystem fs = FileSystem.get(conf);
            FileUtil.copy(fs, path, fs, destPath, false, conf);
            return null;
          }
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return;
    }
    // TODO: Currently no support for shallow copy; however, this is
    // only relevant if the source resource is a collection
    throw new DavException(DavServletResponse.SC_FORBIDDEN,
      "Shallow copies are not supported");
  }

  @Override
  public boolean exists() {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Testing existence of '" + path + "'");
      }
      return user.doAs(new PrivilegedExceptionAction<Boolean>() {
        public Boolean run() throws Exception {
          return FileSystem.get(conf).exists(path);
        }            
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DavResource getCollection() {
    if (path.depth() == 0) {
      return null;
    }
    DavResourceLocator newLocator =
      locator.getFactory().createResourceLocator(locator.getPrefix(),
        path.getParent().toUri().getPath());
    try {
      HDFSResource resource = (HDFSResource)
        factory.createResource(newLocator, getSession());
      resource.user = this.user;
      return resource;
    } catch (DavException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getComplianceClass() {
    return COMPLIANCE_CLASS;
  }

  @Override
  public String getDisplayName() {
    return path.getName();
  }

  @Override
  public DavResourceFactory getFactory() {
    return factory;
  }

  @Override
  public String getHref() {
    StringBuilder sb = new StringBuilder();
    Path p = this.path;
    while (p != null && !("".equals(p.getName()))) {
      sb.insert(0, p.getName());
      sb.insert(0, "/");
      p = p.getParent();
    }
    if (sb.length() == 0) {
      sb.insert(0, "/");
    }
    return sb.toString();
  }

  @Override
  public DavResourceLocator getLocator() {
    return locator;
  }

  @Override
  public ActiveLock getLock(final Type type, final Scope scope) {
    return lockManager.getLock(type, scope, this);
  }

  @Override
  public ActiveLock[] getLocks() {
    return new ActiveLock[0];
  }

  @Override
  public DavResourceIterator getMembers() {
    List<DavResource> list = new ArrayList<DavResource>();
    try {
      FileStatus[] stat = user.doAs(new PrivilegedExceptionAction<FileStatus[]>() {
        public FileStatus[] run() throws Exception {
          return FileSystem.get(conf).listStatus(path);
        }
      });
      if (stat != null) {
        for (FileStatus s: stat) {
          Path p = s.getPath();
          DavResourceLocator resourceLocator =
            locator.getFactory().createResourceLocator(locator.getPrefix(),
              locator.getWorkspacePath(), p.toString(), false);
          try {
            HDFSResource resource = (HDFSResource)
              factory.createResource(resourceLocator, getSession());
            resource.user = this.user;
            list.add(resource);
          } catch (DavException ex) {
            LOG.warn("Exception adding resource '" + p.toUri().getPath() +
              "' to iterator");
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return new DavResourceIteratorImpl(list);
  }

  @Override
  public long getModificationTime() {
    try {
      return user.doAs(new PrivilegedExceptionAction<Long>() {
        public Long run() throws Exception {
          return FileSystem.get(conf).getFileStatus(path).getModificationTime();
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void populateProperties() {
    if (properties != null) {
      return;
    }
    properties = new DavPropertySet();
    FileStatus stat = null;
    try {
      stat = user.doAs(new PrivilegedExceptionAction<FileStatus>() {
        public FileStatus run() throws Exception {
          return FileSystem.get(conf).getFileStatus(getPath());
        }
      });
    } catch (IOException ex) {
      LOG.warn(StringUtils.stringifyException(ex));
    } catch (InterruptedException e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
    if (stat != null) {
      properties.add(new DefaultDavProperty(DavPropertyName.GETCONTENTLENGTH,
        stat.getLen()));
      SimpleDateFormat simpleFormat =
        (SimpleDateFormat)DavConstants.modificationDateFormat.clone();
      simpleFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
      Date date = new Date(stat.getModificationTime());
      properties.add(new DefaultDavProperty(DavPropertyName.GETLASTMODIFIED,
        simpleFormat.format(date)));
      properties.add(new DefaultDavProperty(SecurityConstants.OWNER,
        stat.getOwner()));
      properties.add(new DefaultDavProperty(SecurityConstants.GROUP,
        stat.getGroup()));
      // TODO: Populate DAV property SecurityConstants.CURRENT_USER_PRIVILEGE_SET
    }
    if (getDisplayName() != null) {
      properties.add(new DefaultDavProperty(DavPropertyName.DISPLAYNAME,
        getDisplayName()));
    }
    if (isCollection()) {
      properties.add(new ResourceType(ResourceType.COLLECTION));
      // Windows XP support
      properties.add(new DefaultDavProperty(DavPropertyName.ISCOLLECTION, "1"));
    } else {
      properties.add(new ResourceType(ResourceType.DEFAULT_RESOURCE));
      // Windows XP support
      properties.add(new DefaultDavProperty(DavPropertyName.ISCOLLECTION, "0"));
    }
  }

  @Override
  public DavPropertySet getProperties() {
    synchronized (this) {
      if (properties == null) {
        populateProperties();
      }
      return properties;
    }
  }

  @Override
  public DavProperty<?> getProperty(final DavPropertyName name) {
    synchronized (this) {
      if (properties == null) {
        populateProperties();
      }
      return properties.get(name);
    }
  }

  @Override
  public DavPropertyName[] getPropertyNames() {
    synchronized (this) {
      if (properties == null) {
        populateProperties();
      }
      return properties.getPropertyNames();
    }
  }

  @Override
  public String getResourcePath() {
    return locator.getResourcePath();
  }

  @Override
  public DavSession getSession() {
    return session;
  }

  @Override
  public String getSupportedMethods() {
    return SUPPORTED_METHODS;
  }

  @Override
  public boolean hasLock(final Type type, final Scope scope) {
    return false;
  }

  @Override
  public boolean isCollection() {
    try {
      return user.doAs(new PrivilegedExceptionAction<Boolean>() {
        public Boolean run() throws Exception {
          return FileSystem.get(conf).getFileStatus(path).isDir();
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isLockable(final Type type, final Scope scope) {
    return false;
  }

  @Override
  public ActiveLock lock(final LockInfo reqLockInfo) throws DavException {
    return lockManager.createLock(reqLockInfo, this);
  }

  @Override
  public void move(final DavResource resource) throws DavException {
    final HDFSResource dfsResource = (HDFSResource)resource;
    final Path destPath = dfsResource.getPath();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Moving '" + path.toUri().getPath() + "' to '" +
        destPath.toUri().getPath() + "'");
    }
    try {
      user.doAs(new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          FileSystem.get(conf).rename(path, destPath);
          return null;
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ActiveLock refreshLock(final LockInfo reqLockInfo,
      final String lockToken) throws DavException {
    return lockManager.refreshLock(reqLockInfo, lockToken, this);
  }

  @Override
  public void removeMember(final DavResource resource) throws DavException {
    final HDFSResource dfsResource = (HDFSResource)resource;
    final Path destPath = dfsResource.getPath();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting '" + destPath.toUri().getPath() + "'");
      }
      boolean success = user.doAs(new PrivilegedExceptionAction<Boolean>() {
        public Boolean run() throws Exception {
          return FileSystem.get(conf).delete(destPath, true);
        }
      });
      if (!success) {
        throw new DavException(DavServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void removeProperty(final DavPropertyName name) throws DavException {
    synchronized (this) {
      if (properties == null) {
        populateProperties();
      }
      properties.remove(name);
    }
  }

  @Override
  public void setProperty(final DavProperty<?> property) throws DavException {
    synchronized (this) {
      if (properties == null) {
        populateProperties();
      }
      properties.add(property);
    }
  }

  @Override
  public void spool(final OutputContext context) throws IOException {
    if (!isCollection()) try {
      user.doAs(new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          InputStream input = FileSystem.get(conf).open(path);
          try {
            IOUtils.copyBytes(input, context.getOutputStream(), conf, false);
          } finally {
            input.close();
          }
          return null;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void unlock(final String lockToken) throws DavException { }

}
