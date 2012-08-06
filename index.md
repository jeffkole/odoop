---
layout: main
title: Welcome
---

Welcome to Odoop, a collection of libraries built by [Opower](http://opower.com) to make working
within the Hadoop ecosystem easier.

### Documentation

* [Release Notes](RELEASE_NOTES.html)
* [Query Planner](docs/query-planner.html)
* [API documents](docs/api/index.html)

### Building & Usage

As Odoop is still in the development stages, there is not a released version of the full library yet. To build the library
on your own, clone this repository and use Maven: `mvn clean install`. Under the `hbase-filters` module, there will be a jar
named `deployed-filter-*-bin.jar` that holds the class files necessary to run the `DeployedFilter` and `QueryPlanner`
functionality on the HBase region servers.  Deploy this jar to your HBase region servers and include it on their classpath.

The other jars can be used through standard Maven dependency management:

    <dependency>
      <groupId>com.opower</groupId>
      <artifact>hbase-filters</artifact>
    </dependency>

    <dependency>
      <groupId>com.opower</groupId>
      <artifact>odoop-test</artifact>
      <scope>test</scope>
    </dependency>

### Contributors

* [Jeff Kolesky](http://github.com/jeffkole)

### License

Odoop is licensed under the [Apache License version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
