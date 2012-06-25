package com.opower.hadoop.hbase.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.Random;

import com.opower.hadoop.fs.HdfsClassLoader;

/**
 * Responsible for managing the lifecycle of a {@link DeployedFilter} and
 * more importantly, the {@link Filter} it wraps.
 *
 * @author jeff@opower.com
 */
public class DeployedFilterManager {
    /**
     * A configuration option to specify the base directory where jars will
     * be copied to in HDFS temporarily.
     */
    public static final String DEPLOY_FILTER_PATH = "deployed.filter.path";

    /**
     * The default path to use for deployed jars.
     */
    public static final String DEPLOY_FILTER_DEFAULT_PATH =
        "/tmp/com.opower.hadoop.hbase.DeployedFilterManager";

    private static final Log LOG = LogFactory.getLog(DeployedFilterManager.class);
    private static final Random RANDOM = new Random();
    private static final DeployedFilterMetrics METRICS =
        new DeployedFilterMetrics();

    private final Configuration configuration;

    /**
     * Since the {@link DeployedFilterManager} writes a jar file to HDFS,
     * it needs to have access to a {@link Configuration}
     *
     * @param configuration the HDFS configuration
     */
    public DeployedFilterManager(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Deploys a {@link Filter} to the HBase cluster by finding the jar file
     * the class is defined in and copying it to a location in HDFS.  The
     * location is unique for each deployment so that multiple operations will
     * not interfere with each other.  To ensure that the deployed filter is
     * cleaned up, you must call {@link #undeployFilter} when the operation
     * is complete.  Use the filter returned by this method as the one set on
     * a {@link org.apache.hadoop.hbase.client.Scan}.
     *
     * @param wrappedFilter the {@link Filter} to deploy to the cluster
     * @return a filter representing the deployed one
     * @throws IOException in case the filter cannot be deployed
     */
    public DeployedFilter deployFilter(Filter wrappedFilter)
        throws IOException {
        String filterJarUri = findContainingJar(wrappedFilter.getClass());
        if (filterJarUri == null) {
            throw new IllegalArgumentException(String.format(
                        "Unable to find jar for filter class %s",
                        wrappedFilter.getClass()));
        }
        Path localJarPath = new Path(filterJarUri);
        Path remoteJarPath =
            new Path(constructRemoteJarPath(this.configuration),
                    localJarPath.getName());
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                        "Deploying filter class from %s to %s",
                        localJarPath, remoteJarPath));
        }
        FileSystem.get(this.configuration)
            .copyFromLocalFile(localJarPath, remoteJarPath);

        return new DeployedFilter(remoteJarPath, wrappedFilter);
    }

    /**
     * Completes the lifecycle of a deployed filter by cleaning up the
     * resources used during the deploy.  This method removes the jar file
     * from HDFS as well as the containing directory created to hold it.
     *
     * @param filter the {@link DeployedFilter} that needs undeploying
     * @throws IOException in case the filter cannot be undeployed
     */
    public void undeployFilter(DeployedFilter filter) throws IOException {
        Path remoteJarPath = filter.getRemoteJarPath();
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Deleting remote jar from %s",
                        remoteJarPath));
        }
        // The remoteJarPath is a path to a jar, so delete it, and then delete
        // the directory that contains it, since that is meant to be temporary
        // as well
        remoteJarPath.getFileSystem(this.configuration)
            .delete(remoteJarPath, true);
        Path parent = remoteJarPath.getParent();
        parent.getFileSystem(this.configuration).delete(parent, true);
    }

    /**
     * Loads a filter from the jar in HDFS.  Meant to be called only by
     * {@link DeployedFilter}.  Since a new {@link ClassLoader} is created
     * each time, just to load this one class, when the class is no longer
     * needed and can be garbage collected, then the class loader will be
     * collected as well.  If a new version of the {@link Filter} is desired,
     * then it will be loaded the next time the filter is run, because it
     * will be loaded by another instance of a class loader that is reading
     * from a new version of the jar deployed into the cluster.
     *
     * @param jarPath the {@link Path} to the jar in HDFS
     * @param filterClassName the binary name of the filter to load
     * @return a new instance of the {@link Filter} class
     * @throws RuntimeException if anything goes wrong
     */
    static Filter loadFilter(Path jarPath, String filterClassName) {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Loading new filter %s from %s",
                    filterClassName, jarPath));
            }
            // TODO: is creating a new configuration sufficient, or
            // does it need to be serialized from the DeployedFilter?
            ClassLoader filterLoader =
                new HdfsClassLoader(new Configuration(), jarPath);
            METRICS.classLoaderInstantiated(filterLoader);
            Class<?> filterClass =
                Class.forName(filterClassName, true, filterLoader);
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Filter class: %s@%x",
                            filterClass.getName(),
                            System.identityHashCode(filterClass)));
            }
            if (!filterClass.getClassLoader().equals(filterLoader)) {
                LOG.warn(String.format("Filter class (%s@%x) loaded by " +
                    "unexpected class loader (%s), instead of %s.  This " +
                    "is a likely result of the class being present in " +
                    "another resource that a parent class loader has " +
                    "access to.  You may not be using the freshest version " +
                    "of your filter in this case.",
                    filterClass.getName(),
                    System.identityHashCode(filterClass),
                    filterClass.getClassLoader(),
                    filterLoader));
            }
            else {
                METRICS.filterDynamicallyLoaded(filterClass);
            }
            METRICS.filterInstantiated(filterClass);
            return (Filter)filterClass.newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Find a jar that contains a class of the same name, if any.
     * It will return a jar file, even if that is not the first thing
     * on the class path that has a class with the same name.
     *
     * Note: This is copied directly from
     * {@link org.apache.hadoop.mapred.JobConf.findContainingJar}.
     *
     * @param clazz the class to find
     * @return a URL-like String to a jar file that contains the class, or null
     * @throws IOException if the class resources cannot be read
     */
    private static String findContainingJar(Class clazz) throws IOException {
        ClassLoader loader = clazz.getClassLoader();
        String classFile = clazz.getName().replaceAll("\\.", "/") + ".class";
        for (Enumeration itr = loader.getResources(classFile);
                itr.hasMoreElements();) {
            URL url = (URL)itr.nextElement();
            if ("jar".equals(url.getProtocol())) {
                String toReturn = url.getPath();
                if (toReturn.startsWith("file:")) {
                    toReturn = toReturn.substring("file:".length());
                }
                // URLDecoder is a misnamed class, since it actually decodes
                // x-www-form-urlencoded MIME type rather than actual
                // URL encoding (which the file path has). Therefore it would
                // decode +s to ' 's which is incorrect (spaces are actually
                // either unencoded or encoded as "%20"). Replace +s first, so
                // that they are kept sacred during the decoding process.
                toReturn = toReturn.replaceAll("\\+", "%2B");
                toReturn = URLDecoder.decode(toReturn, "UTF-8");
                return toReturn.replaceAll("!.*$", "");
            }
        }
        return null;
    }

    /**
     * Construct a path to temporarily copy jars during deployment; the
     * path will be roughly unique, as it is based on the current time as well
     * as a random long.
     */
    private static Path constructRemoteJarPath(Configuration configuration)
        throws IOException {
        String baseDirectory = configuration.get(
                DEPLOY_FILTER_PATH, DEPLOY_FILTER_DEFAULT_PATH);
        baseDirectory +=
            "/" + String.valueOf(System.currentTimeMillis()) +
            "-" + String.valueOf(RANDOM.nextLong());
        Path remoteJarPath = new Path(baseDirectory);
        FileSystem fs = FileSystem.get(configuration);
        remoteJarPath = remoteJarPath.makeQualified(fs);
        fs.mkdirs(remoteJarPath);
        return remoteJarPath;
    }
}
