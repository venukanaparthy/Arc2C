package com.esri;

import java.net.InetSocketAddress;
import java.util.List;


public class CnxConfig {
	private List<InetSocketAddress> hostAddresses;
	private String keyspace;
	private String table;
	public CnxConfig(List<InetSocketAddress> hostAddresses, String keyspace, String table) {
		this.setHostAddresses(hostAddresses);
		this.setKeyspace(keyspace);
		this.setTable(table);		
	}	
	public String getKeyspace() {
		return keyspace;
	}
	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public List<InetSocketAddress> getHostAddresses() {
		return hostAddresses;
	}
	public void setHostAddresses(List<InetSocketAddress> hostAddresses) {
		this.hostAddresses = hostAddresses;
	}
}
