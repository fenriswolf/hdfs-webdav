package com.trendmicro.hdfs.webdav;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.yammer.metrics.jetty.InstrumentedSelectChannelConnector;
import com.yammer.metrics.reporting.GangliaReporter;
import com.yammer.metrics.web.DefaultWebappMetricsFilter;

public class Main {

  private static final Log LOG = LogFactory.getLog(Main.class);

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Usage: webdav [options]", "", options,
      "\nTo run the WebDAV server as a daemon, execute " +
      "bin/hadoop-daemon.sh start " + Main.class.getCanonicalName() +
      " [options]\n", true);
    System.exit(exitCode);
  }

  private static InetSocketAddress getAddress(Configuration conf) {
    return NetUtils.createSocketAddr(
      conf.get("webdav.bind.address", "0.0.0.0"),
      conf.getInt("webdav.port", 8080));
  }

  public static void main(String[] args) {

    HDFSWebDAVServlet servlet = HDFSWebDAVServlet.getServlet();
    Configuration conf = servlet.getConfiguration();

    // Process command line 

    Options options = new Options();
    options.addOption("p", "port", true, "Port to bind to [default: 8080]");
    options.addOption("b", "bind-address", true, "Address bind to [default: 0.0.0.0]");
    options.addOption("g", "ganglia", true,
      "Send Ganglia metrics to host:port [default: none]");

    String gangliaHost = null;
    int gangliaPort = 8649;
    CommandLine cmd = null;
    try {
      cmd = new PosixParser().parse(options, args);
    } catch (ParseException e) {
      printUsageAndExit(options, -1);
    }
    if (cmd.hasOption('b')) {
      conf.set("webdav.bind.address", cmd.getOptionValue('b'));
    }
    if (cmd.hasOption('p')) {
      conf.setInt("webdav.port", Integer.valueOf(cmd.getOptionValue('p')));
    }
    if (cmd.hasOption('g')) {
      String val = cmd.getOptionValue('g');
      if (val.indexOf(':') != -1) {
        String[] split = val.split(":");
        gangliaHost = split[0];
        gangliaPort = Integer.valueOf(split[1]);
      } else {
        gangliaHost = val;
      }
    }

    // Log in the server principal from keytab

    InetSocketAddress addr = getAddress(conf);    
    UserGroupInformation.setConfiguration(conf);
    try {
      SecurityUtil.login(conf, "webdav.keytab.file", 
        "webdav.kerberos.principal", addr.getHostName());
    } catch (Exception e) {
      LOG.fatal("Unable to log in", e);
      System.err.println("Unable to log in");
      System.exit(-1);
    }

    // Set up embedded Jetty

    Server server = new Server();

    server.setSendServerVersion(false);
    server.setSendDateHeader(false);
    server.setStopAtShutdown(true);

    // Set up connector
    Connector connector =
      new InstrumentedSelectChannelConnector(addr.getPort());
    connector.setHost(addr.getHostName());
    server.addConnector(connector);
    LOG.info("Listening on " + addr);

    // Set up context
    ServletContextHandler context =
      new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
        // WebDAV servlet
    context.addServlet(new ServletHolder(servlet), "/*");
        // metrics instrumentation filter
    context.addFilter(new FilterHolder(new DefaultWebappMetricsFilter()),
      "/*", 0);
        // auth filter
    context.addFilter(new FilterHolder(new AuthFilter(conf)), "/*", 0);

    // Set up Ganglia metrics reporting
    if (gangliaHost != null) {
      GangliaReporter.enable(1, TimeUnit.MINUTES, gangliaHost, gangliaPort);
    }

    // Start and join the server thread    
    try {
      server.start();
      server.join();
    } catch (Exception e) {
      LOG.fatal("Failed to start Jetty", e);
      System.err.println("Failed to start Jetty");
      System.exit(-1);
    }
  }

}
