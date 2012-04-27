package com.trendmicro.hdfs.webdav;

import java.io.IOException;
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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;

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
      conf.get("hadoop.webdav.bind.address", "0.0.0.0"),
      conf.getInt("hadoop.webdav.port", 8080));
  }

  public static void main(String[] args) {

    HDFSWebDAVServlet servlet = HDFSWebDAVServlet.getServlet();
    Configuration conf = servlet.getConfiguration();

    // Process command line 

    Options options = new Options();
    options.addOption("d", "debug", false, "Enable debug logging");
    options.addOption("p", "port", true, "Port to bind to [default: 8080]");
    options.addOption("b", "bind-address", true,
      "Address or hostname to bind to [default: 0.0.0.0]");
    options.addOption("g", "ganglia", true,
      "Send Ganglia metrics to host:port [default: none]");

    CommandLine cmd = null;
    try {
      cmd = new PosixParser().parse(options, args);
    } catch (ParseException e) {
      printUsageAndExit(options, -1);
    }

    if (cmd.hasOption('d')) {
      Logger rootLogger = Logger.getLogger("com.trendmicro");
      rootLogger.setLevel(Level.DEBUG);
    }

    if (cmd.hasOption('b')) {
      conf.set("hadoop.webdav.bind.address", cmd.getOptionValue('b'));
    }

    if (cmd.hasOption('p')) {
      conf.setInt("hadoop.webdav.port",
        Integer.valueOf(cmd.getOptionValue('p')));
    }

    String gangliaHost = null;
    int gangliaPort = 8649;
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

    InetSocketAddress addr = getAddress(conf);

    // Log in the server principal from keytab

    UserGroupInformation.setConfiguration(conf);
    if (UserGroupInformation.isSecurityEnabled()) try {
      SecurityUtil.login(conf,
        conf.get("hadoop.webdav.server.kerberos.keytab"),
        conf.get("hadoop.webdav.server.kerberos.principal"),
        addr.getHostName());
    } catch (IOException e) {
      LOG.fatal("Could not log in", e);
      System.err.println("Could not log in");
      System.exit(-1);
    }

    // Set up embedded Jetty

    Server server = new Server();

    server.setSendServerVersion(false);
    server.setSendDateHeader(false);
    server.setStopAtShutdown(true);

    // Set up connector
    Connector connector = new SelectChannelConnector();
    connector.setPort(addr.getPort());
    connector.setHost(addr.getHostName());
    server.addConnector(connector);
    LOG.info("Listening on " + addr);

    // Set up context
    Context context = new Context(server, "/", Context.SESSIONS);
        // WebDAV servlet
    ServletHolder servletHolder = new ServletHolder(servlet);
    servletHolder.setInitParameter("authenticate-header",
      "Basic realm=\"Hadoop WebDAV Server\"");
    context.addServlet(servletHolder, "/*");
        // metrics instrumentation filter
    context.addFilter(new FilterHolder(new DefaultWebappMetricsFilter()),
      "/*", 0);
        // auth filter
    context.addFilter(new FilterHolder(new AuthFilter(conf)), "/*", 0);
    server.setHandler(context);

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
