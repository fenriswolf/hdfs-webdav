package com.trendmicro.hdfs.webdav;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.jackrabbit.webdav.DavException;
import org.apache.jackrabbit.webdav.DavMethods;
import org.apache.jackrabbit.webdav.DavResource;
import org.apache.jackrabbit.webdav.DavResourceFactory;
import org.apache.jackrabbit.webdav.DavResourceLocator;
import org.apache.jackrabbit.webdav.DavServletRequest;
import org.apache.jackrabbit.webdav.DavServletResponse;
import org.apache.jackrabbit.webdav.DavSession;
import org.apache.jackrabbit.webdav.simple.ResourceConfig;

public class HDFSResourceFactory implements DavResourceFactory {

  private ResourceConfig resourceConf;
  private Configuration conf;

  public HDFSResourceFactory(ResourceConfig resourceConf, Configuration conf) {
    this.resourceConf = resourceConf;
    this.conf = conf;
  }

  @Override
  public DavResource createResource(DavResourceLocator locator,
      DavSession session) throws DavException {
    try {
      return new HDFSResource(this, locator, session, resourceConf, conf);
    } catch (IOException e) {
      throw new DavException(DavServletResponse.SC_INTERNAL_SERVER_ERROR,
        e.getMessage());
    }
  }

  @Override
  public DavResource createResource(DavResourceLocator locator,
      DavServletRequest request, DavServletResponse response)
      throws DavException {
    try {
      return new HDFSResource(this, locator, request.getDavSession(),
        resourceConf, conf, DavMethods.isCreateCollectionRequest(request));
    } catch (IOException e) {
      throw new DavException(DavServletResponse.SC_INTERNAL_SERVER_ERROR,
        e.getMessage());
    }
  }

}
