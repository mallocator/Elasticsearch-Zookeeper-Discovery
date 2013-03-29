Elasticsearch-Zookeeper-Discovery
=================================

A discovery module for Elasticsearch that allows nodes to find each other via communication over a Zookeeper cluster. Of course you need a running ZK server to get this working.

# Building

To build the plugin you need to have maven installed. With that in mind simply check out the project and run "mvn package" in the project directory. The plugin should then be available under target/release as a .zip file.

# Installation

Just copy the .zip file on the elasticsearch server should be using the plugin and run the "plugin" script coming with elasticsearch in the bin folder.

An Exmaple how one would call the plugin script:

	/my/elasticsearch/bin/plugin install zkdk -url file:///path/to/plugin/zk-discovery.zip

The plugin needs to be installed on all nodes of the ES cluster.