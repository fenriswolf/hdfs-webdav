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
  private final FileSystem fs;
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
    this.fs = FileSystem.get(conf);
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

  public void setProxyUser(String user) throws IOException {
    this.user = UserGroupInformation.createProxyUser(user,
      UserGroupInformation.getLoginUser());
  }

  @Override
  public void addLockManager(LockManager lockManager) {
    this.lockManager = lockManager;
  }

  @Override
  public void addMember(DavResource resource, InputContext context)
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
        boolean success = user.doAs(
          new PrivilegedExceptionAction<Boolean>() {
            public Boolean run() throws Exception {
              return fs.mkdirs(destPath);
            }
          });
        if (!success) {
          throw new DavException(DavServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Creating new file '" + destPath.toUri().getPath() + "'");
        }
        if (!context.hasStream() || context.getContentLength() < 0) {
          boolean success = user.doAs(
            new PrivilegedExceptionAction<Boolean>() {
              public Boolean run() throws Exception {
                return fs.createNewFile(destPath);
              }
            });
          if (!success) {
            throw new DavException(DavServletResponse.SC_CONFLICT);
          }
        } else {
          OutputStream out = user.doAs(
            new PrivilegedExceptionAction<OutputStream>() {
              public OutputStream run() throws Exception {
                return fs.create(destPath);
              }
            });
          InputStream in = context.getInputStream();
          IOUtils.copyBytes(in, out, conf, true);
        }
      }
    } catch (IOException e) {
      LOG.warn("Exception creating new resource '" + destPath + "'", e);
      throw new DavException(DavServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while creating new resource '" + destPath + "'", e);
      throw new DavException(DavServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  @Override
  public MultiStatusResponse alterProperties(List<? extends PropEntry> props)
      throws DavException {
    return null;
  }

  @Override
  public void copy(DavResource resource, boolean shallow) throws DavException {
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
        user.doAs(
          new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
              FileUtil.copy(fs, path, fs, destPath, false, conf);
              return null;
            }
          });
      } catch (IOException e) {
        LOG.warn("Exception copying '" + path + "' to '" + destPath + "'", e);
        throw new DavException(DavServletResponse.SC_INTERNAL_SERVER_ERROR);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while copying '" + path + "' to '" + destPath +
          "'", e);
        throw new DavException(DavServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    }
    // TODO: Currently no support for shallow copy; however, this is
    // only relevant if the source resource is a collection
    throw new DavException(DavServletResponse.SC_FORBIDDEN,
      "Shallow copies are not supported");
  }

  @Override
  public boolean exists() {
    try {
      return user.doAs(
        new PrivilegedExceptionAction<Boolean>() {
          public Boolean run() throws Exception {
            return fs.exists(path);
          }            
        });
    } catch (IOException e) {
      LOG.warn("Exception testing existence of '" + path + "'", e);
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while testing existence of '" + path + "'", e);
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
      return factory.createResource(newLocator, getSession());
    } catch (DavException e) {
      LOG.warn(StringUtils.stringifyException(e));
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
  public ActiveLock getLock(Type type, Scope scope) {
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
      FileStatus[] stat = user.doAs(
        new PrivilegedExceptionAction<FileStatus[]>() {
          public FileStatus[] run() throws Exception {
            return fs.listStatus(path);
          }
        });
      if (stat != null) {
        for (FileStatus s: stat) {
          Path p = s.getPath();
          DavResourceLocator resourceLocator =
            locator.getFactory().createResourceLocator(locator.getPrefix(),
              locator.getWorkspacePath(), p.toString(), false);
          try {
            list.add(factory.createResource(resourceLocator, getSession()));
          } catch (DavException ex) {
            LOG.warn("Exception adding resource '" + p.toUri().getPath() +
              "' to iterator");
          }
        }
      }
    } catch (IOException e) {
      LOG.warn(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      LOG.warn(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
    return new DavResourceIteratorImpl(list);
  }

  @Override
  public long getModificationTime() {
    try {
      return user.doAs(new PrivilegedExceptionAction<Long>() {
        public Long run() throws Exception {
          return fs.getFileStatus(path).getModificationTime();
        }
      });
    } catch (IOException e) {
      LOG.warn(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      LOG.warn(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void populateProperties() {
    if (properties != null) {
      return;
    }
    try {
      FileStatus stat = user.doAs(
        new PrivilegedExceptionAction<FileStatus>() {
          public FileStatus run() throws Exception {
            return fs.getFileStatus(getPath());
          }
        });
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
    } catch (IOException ex) {
      LOG.warn(StringUtils.stringifyException(ex));
    } catch (InterruptedException e) {
      LOG.warn(StringUtils.stringifyException(e));
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
  public DavProperty<?> getProperty(DavPropertyName name) {
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
  public boolean hasLock(Type type, Scope scope) {
    return false;
  }

  @Override
  public boolean isCollection() {
    try {
      return user.doAs(
        new PrivilegedExceptionAction<Boolean>() {
          public Boolean run() throws Exception {
            return fs.getFileStatus(path).isDir();
          }
        });
    } catch (Exception e) {
      LOG.warn(StringUtils.stringifyException(e));
      return false;
    }
  }

  @Override
  public boolean isLockable(Type type, Scope scope) {
    return false;
  }

  @Override
  public ActiveLock lock(LockInfo reqLockInfo) throws DavException {
    return lockManager.createLock(reqLockInfo, this);
  }

  @Override
  public void move(DavResource resource) throws DavException {
    final HDFSResource dfsResource = (HDFSResource)resource;
    final Path destPath = dfsResource.getPath();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Moving '" + path.toUri().getPath() + "' to '" +
        destPath.toUri().getPath() + "'");
    }
    try {
      user.doAs(
        new PrivilegedExceptionAction<Void>() {
          public Void run() throws Exception {
            fs.rename(path, destPath);
            return null;
          }
        });
    } catch (IOException e) {
      LOG.warn("Exception moving '" + path + "' to '" + destPath + "'", e);
      throw new DavException(DavServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while moving '" + path + "' to '" + destPath +
        "'", e);
      throw new DavException(DavServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  @Override
  public ActiveLock refreshLock(LockInfo reqLockInfo, String lockToken)
      throws DavException {
    return lockManager.refreshLock(reqLockInfo, lockToken, this);
  }

  @Override
  public void removeMember(DavResource resource) throws DavException {
    final HDFSResource dfsResource = (HDFSResource)resource;
    final Path destPath = dfsResource.getPath();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting '" + destPath.toUri().getPath() + "'");
      }
      boolean success = user.doAs(
        new PrivilegedExceptionAction<Boolean>() {
          public Boolean run() throws Exception {
            return fs.delete(destPath, true);
          }
        });
      if (!success) {
        throw new DavException(DavServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    } catch (IOException e) {
      LOG.warn("Exception deleting resource '" + destPath + "'", e);
      throw new DavException(DavServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while deleting resource '" + destPath + "'", e);
      throw new DavException(DavServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  @Override
  public void removeProperty(DavPropertyName name) throws DavException {
    synchronized (this) {
      if (properties == null) {
        populateProperties();
      }
      properties.remove(name);
    }
  }

  @Override
  public void setProperty(DavProperty<?> property) throws DavException {
    synchronized (this) {
      if (properties == null) {
        populateProperties();
      }
      properties.add(property);
    }
  }

  @Override
  public void spool(OutputContext context) throws IOException {
    if (!isCollection()) try {
      InputStream input = user.doAs(
        new PrivilegedExceptionAction<InputStream>() {
          public InputStream run() throws Exception {
            return fs.open(path);
          }
      });
      try {
        IOUtils.copyBytes(input, context.getOutputStream(), conf, false);
      } finally {
        input.close();
      }
    } catch (IOException e) {
      LOG.warn("Exception spooling resource '" + path.toUri().getPath() +
        "'", e);
      throw e;
    } catch (InterruptedException e) {
      LOG.warn("Interrupted spooling resource '" + path.toUri().getPath() +
        "'", e);
      throw new IOException(e);
    }
  }

  @Override
  public void unlock(String lockToken) throws DavException { }

}
