package com.esri;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;


public enum CassandraClient {

	DB;
	
    private Session session;
    private Cluster cluster;
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClient.class);

    /**
     * Connect to the cassandra database based on the connection configuration provided.
     * Multiple call to this method will have no effects if a connection is already established
     * @param conf the configuration for the connection
     */
    /*public void connect(CnxConfig conf) {
        if (cluster == null && session == null) {
            cluster = Cluster.builder().addContactPointsWithPorts(conf.getHostAddresses()).build();
            session = cluster.connect(conf.getKeyspace());
        }
        Metadata metadata = cluster.getMetadata();
        LOGGER.info("Connected to cluster: " + metadata.getClusterName() + " with partitioner: " + metadata.getPartitioner());
        metadata.getAllHosts().stream().forEach((host) -> {
            LOGGER.info("Cassandra datacenter: " + host.getDatacenter() + " | address: " + host.getAddress() + " | rack: " + host.getRack());
        });
    }*/
    
    /*
     *  Connect with defaults
     */
    public void connect(){
    	
    	ArrayList<InetSocketAddress> adddresss = new ArrayList <InetSocketAddress>();
		adddresss.add(new InetSocketAddress("192.168.75.129", 9042));								
    	if (cluster == null && session == null) {
            cluster = Cluster.builder().addContactPointsWithPorts(adddresss).build();
            session = cluster.connect("vehicle_tracker");
        }
    	
        /*Metadata metadata = cluster.getMetadata();
        LOGGER.info("Connected to cluster: " + metadata.getClusterName() + " with partitioner: " + metadata.getPartitioner());
        metadata.getAllHosts().stream().forEach((host) -> {
            LOGGER.info("Cassandra datacenter: " + host.getDatacenter() + " | address: " + host.getAddress() + " | rack: " + host.getRack());
        });*/
    }

    /**
     * Invalidate and close the session and connection to the cassandra database
     */
    public void shutdown() {
        //LOGGER.info("Shutting down the whole cassandra cluster");
        if (null != session) {
            session.close();
        }
        if (null != cluster) {
            cluster.close();
        }
    }

    public Session getSession() {
        if (session == null) {
            throw new IllegalStateException("No connection initialized");
        }
        return session;
    }
}