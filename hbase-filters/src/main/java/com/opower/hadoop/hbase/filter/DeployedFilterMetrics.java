package com.opower.hadoop.hbase.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.HashSet;
import java.util.Set;

/**
 * Collects and emits metrics for the use of the {@link DeployedFilter}.
 * Metrics are added to the "extensions" context under the record named
 * "deployedFilter".
 *
 * @author jeff@opower.com
 */
class DeployedFilterMetrics implements Updater {
    private static final Log LOG = LogFactory.getLog(DeployedFilterMetrics.class);

    private final MetricsRegistry registry = new MetricsRegistry();
    private final MetricsRecord metricsRecord;

    private final MetricsLongValue filtersDynamicallyLoaded = new MetricsLongValue("filtersDynamicallyLoaded", this.registry);
    private final MetricsLongValue filtersCollected = new MetricsLongValue("filtersCollected", this.registry);

    private final MetricsLongValue classLoadersInstantiated = new MetricsLongValue("classLoadersInstantiated", this.registry);
    private final MetricsLongValue classLoadersCollected = new MetricsLongValue("classLoadersCollected", this.registry);

    /**
     * Keep track of phantom references to class loaders to detect possible memory leaks
     */
    private final ReferenceQueue<ClassLoader> classLoaderQueue = new ReferenceQueue<ClassLoader>();
    /**
     * Keep track of phantom references to filter classes to detect possible memory leaks
     */
    private final ReferenceQueue<Class> filterClassQueue = new ReferenceQueue<Class>();

    private final Set<Reference> classLoaders = new HashSet<Reference>();
    private final Set<Reference> filterClasses = new HashSet<Reference>();

    DeployedFilterMetrics() {
        MetricsContext context = MetricsUtil.getContext("extensions");
        metricsRecord = MetricsUtil.createRecord(context, "deployedFilter");
        context.registerUpdater(this);
    }

    synchronized void filterDynamicallyLoaded(Class filterClass) {
        this.filtersDynamicallyLoaded.set(this.filtersDynamicallyLoaded.get() + 1);
        // Create a phantom reference so we can track when the filter class is garbage collected
        this.filterClasses.add(new PhantomReference<Class>(filterClass, this.filterClassQueue));
    }

    synchronized void classLoaderInstantiated(ClassLoader classLoader) {
        this.classLoadersInstantiated.set(this.classLoadersInstantiated.get() + 1);
        // Create a phantom reference so we can track when the class loader is garbage collected
        this.classLoaders.add(new PhantomReference<ClassLoader>(classLoader, this.classLoaderQueue));
    }

    void filterInstantiated(Class filterClass) {
        String filterClassName = filterClass.getName();
        String metricName = "instantiated:" + filterClassName;
        synchronized (this) {
            MetricsLongValue metric = (MetricsLongValue)this.registry.get(metricName);
            if (metric == null) {
                metric = new MetricsLongValue(metricName, this.registry);
            }
            metric.set(metric.get() + 1);
        }
    }

    public synchronized void doUpdates(MetricsContext caller) {
        synchronized (this) {
            // Determine how many classes and class loaders have been
            // collected and add those counts to the respective metrics
            incrementCollectedMetrics(this.classLoaders, this.classLoaderQueue, this.classLoadersCollected);
            incrementCollectedMetrics(this.filterClasses, this.filterClassQueue, this.filtersCollected);

            for (MetricsBase metric : this.registry.getMetricsList()) {
                metric.pushMetric(this.metricsRecord);
            }
        }
        this.metricsRecord.update();
    }

    private void incrementCollectedMetrics(Set<Reference> references, ReferenceQueue queue, MetricsLongValue metric) {
        int count = 0;
        Reference ref = null;
        while ((ref = queue.poll()) != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Found reference to be collected: %s; %s", ref, ref.get()));
            }
            // be sure to clear the references so that they can actually be collected by the garbage collector
            ref.clear();
            references.remove(ref);
            metric.set(metric.get() + 1);
            count++;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("%d references collected for %s", count, metric.getName()));
        }
    }
}
