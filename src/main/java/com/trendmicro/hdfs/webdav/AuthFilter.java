package com.trendmicro.hdfs.webdav;

import java.util.Map;
import java.util.Properties;

import javax.servlet.FilterConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;

public class AuthFilter extends AuthenticationFilter {

  private static final String CONF_PREFIX = "webdav.authentication.";
  private Configuration conf;

  public AuthFilter(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Return an Alfredo configuration from the webdav authentication conf
   * properties
   */
  @Override
  protected Properties getConfiguration(String configPrefix,
      FilterConfig filterConfig) {
    Properties props = new Properties();
    props.setProperty(AuthenticationFilter.COOKIE_PATH, "/");
    for (Map.Entry<String, String> e: conf) {
      String name = e.getKey();
      if (name.startsWith(CONF_PREFIX)) {
        String value = conf.get(name);
        name = name.substring(CONF_PREFIX.length());
        props.setProperty(name, value);
      }
    }
    return props;
  }

}
