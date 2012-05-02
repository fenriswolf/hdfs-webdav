This is a simple WebDAV servlet, based on Apache Jackrabbit, derived from
the iponweb HDFS over WebDAV project.

Apache Jackrabbit: http://jackrabbit.apache.org/

iponweb HDFS: http://www.hadoop.iponweb.net/Home/hdfs-over-webdav

To deploy:

1. Create the UNIX user that will run the WebDAV server, e.g. "webdav".

2. Create per-host Kerberos credentials for this service user and create per host keytab files. You will also need to add a per host Kerberos credential for "HTTP" to the keytab. (This is needed by the SPNEGO authentication filter, per the SPNEGO spec.)

3. Edit the Hadoop core-site.xml and define the Unix user that will run the WebDAV server as a proxyuser. For example, if the user is 'webdav':

    <pre>
    ...
    &lt;property&gt;
        &lt;name&gt;hadoop.proxyuser.webdav.hosts&lt;/name&gt;
        &lt;value&gt;host1,host2,hostN&lt;/value&gt;
    &lt;/property&gt;
    &lt;property&gt;
        &lt;name&gt;hadoop.proxyuser.webdav.groups&lt;/name&gt;
        &lt;value&gt;*&lt;/value&gt;
    &lt;/property&gt;
    ...
    </pre>

    This configuration must be done on the NameNode and then the NameNode must be restarted.

4. Untar the WebDAV gateway package onto the hosts defined in hadoop.proxyuser.webdav.hosts. Symlink core-site.xml and hdfs-site.xml from the Hadoop configuration into its conf/ directory.

5. Deploy the per-host service keytab into the conf/ directory.

6. Edit the webdav-site.xml file in its conf/ directory as needed:

    <pre>
    &lt;property&gt;
        &lt;name&gt;hadoop.webdav.bind.address&lt;/name&gt;
        &lt;value&gt;0.0.0.0&lt;/value&gt;
    &lt;/property&gt;
    &lt;property&gt;
        &lt;name&gt;hadoop.webdav.port&lt;/name&gt;
        &lt;value&gt;8080&lt;/value&gt;
    &lt;/property&gt;
    &lt;property&gt;
        &lt;name&gt;hadoop.webdav.server.kerberos.principal&lt;/name&gt;
        &lt;value&gt;webdav/_HOST@HADOOP.LOCALDOMAIN&lt;/value&gt;
    &lt;/property&gt;
    &lt;property&gt;
        &lt;name&gt;hadoop.webdav.server.kerberos.keytab&lt;/name&gt;
        &lt;value&gt;/path/to/webdav.keytab&lt;/value&gt;
    &lt;/property&gt;
    &lt;property&gt;
        &lt;name&gt;hadoop.webdav.authentication.type&lt;/name&gt;
        &lt;value&gt;kerberos&lt;/value&gt;
    &lt;/property&gt;
    &lt;property&gt;
        &lt;name&gt;hadoop.webdav.authentication.kerberos.principal&lt;/name&gt;
        &lt;value&gt;HTTP/_HOST@HADOOP.LOCALDOMAIN&lt;/value&gt;
    &lt;/property&gt;
    &lt;property&gt;
        &lt;name&gt;hadoop.webdav.authentication.kerberos.keytab&lt;/name&gt;
        &lt;value&gt;/path/to/webdav.keytab&lt;/value&gt;
    &lt;/property&gt;
    </pre>

This is a configuration for a secure cluster. Given this example, if the KDC is running and per host principals for 'webdav' and 'HTTP' were added to the local keytab, then you should see something like the below logged at startup:

    <pre>
    12/05/02 14:53:52 INFO security.UserGroupInformation: Login successful for user webdav/ip-10-177-2-205.us-west-1.compute.internal@HADOOP.LOCALDOMAIN using keytab file /etc/hadoop/conf/hdfs.keytab
    12/05/02 14:53:52 INFO webdav.Main: Listening on 0.0.0.0/0.0.0.0:8080
    12/05/02 14:53:52 INFO server.KerberosAuthenticationHandler: Initialized, principal [HTTP/_HOST@HADOOP.LOCALDOMAIN] from keytab [/etc/hadoop/conf/hdfs.keytab]
    12/05/02 14:53:52 INFO server.AbstractWebdavServlet: authenticate-header = Basic realm="Hadoop WebDAV Server"
    12/05/02 14:53:52 INFO mortbay.log: Started SelectChannelConnector@0.0.0.0:8080
    </pre>

If instead you are running on some test cluster that does not have security enabled, check out the settings in conf/webdav-default.xml. The default authentication type is "simple" and anonymous access is allowed. If you want to run this way, then just don't override those config properties (you can probably just skip creation of a webdav-site.xml file).
