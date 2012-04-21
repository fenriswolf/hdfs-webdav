package com.trendmicro.hdfs.webdav;

import org.apache.jackrabbit.webdav.DavException;
import org.apache.jackrabbit.webdav.DavSessionProvider;
import org.apache.jackrabbit.webdav.WebdavRequest;

public class SessionProvider implements DavSessionProvider {

  @Override
  public boolean attachSession(WebdavRequest request) throws DavException {
    return true;
  }

  @Override
  public void releaseSession(WebdavRequest request) { }
}
