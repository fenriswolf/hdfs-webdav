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

import java.util.Map;
import java.util.Properties;

import javax.servlet.FilterConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;

public class AuthFilter extends AuthenticationFilter {

  private static final String CONF_PREFIX = "hadoop.webdav.authentication.";
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
