# Resolve links ($0 may be a softlink) and convert a relative path
# to an absolute path.  NB: The -P option requires bash built-ins
# or POSIX:2001 compliant cd and pwd.
this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$bin/$script"

# the root of the Webdav installation
if [ -z "$WEBDAV_HOME" ]; then
  export WEBDAV_HOME=`dirname "$this"`/..
fi

# double check that our WEBDAV_HOME looks reasonable.
# cding to / here verifies that we have an absolute path, which is
# necessary for the daemons to function properly
if [ -z "$(cd / && ls $WEBDAV_HOME/hdfs-webdav-*.jar $WEBDAV_HOME/build 2>/dev/null)" ]; then
  cat 1>&2 <<EOF
+================================================================+
|      Error: WEBDAV_HOME is not set correctly                   |
+----------------------------------------------------------------+
| Please set your WEBDAV_HOME variable to the absolute path of   |
| the directory that contains hdfs-webdav-VERSION.jar            |
+================================================================+
EOF
  exit 1
fi

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
	  then
	      shift
	      confdir=$1
	      shift
	      WEBDAV_CONF_DIR=$confdir
    fi
fi
 
# Allow alternate conf dir location.
WEBDAV_CONF_DIR="${WEBDAV_CONF_DIR:-$WEBDAV_HOME/conf}"

if [ -f "${WEBDAV_CONF_DIR}/webdav-env.sh" ]; then
  . "${WEBDAV_CONF_DIR}/webdav-env.sh"
fi

# attempt to find java
if [ -z "$JAVA_HOME" ]; then
  for candidate in \
    /usr/lib/jvm/java-6-sun \
    /usr/lib/jvm/java-1.6.0-sun-1.6.0.* \
    /usr/lib/j2sdk1.6-sun \
    /usr/java/jdk1.6* \
    /usr/java/jre1.6* \
    /Library/Java/Home \
    /usr/java/default \
    /usr/lib/jvm/default-java ; do
    if [ -e $candidate/bin/java ]; then
      export JAVA_HOME=$candidate
      break
    fi
  done
  # if we didn't set it
  if [ -z "$JAVA_HOME" ]; then
    cat 1>&2 <<EOF
+======================================================================+
|      Error: JAVA_HOME is not set and Java could not be found         |
+----------------------------------------------------------------------+
| Please download the latest Sun JDK from the Sun Java web site        |
|       > http://java.sun.com/javase/downloads/ <                      |
|                                                                      |
| Webdav requires Java 1.6 or later.                                   |
| NOTE: This script will find Sun Java whether you install using the   |
|       binary or the RPM based installer.                             |
+======================================================================+
EOF
    exit 1
  fi
fi

# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. This interacts badly with the many threads that
# we use in Webdav. Tune the variable down to prevent vmem explosion.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}
