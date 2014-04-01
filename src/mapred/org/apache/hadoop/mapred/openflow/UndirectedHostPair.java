package org.apache.hadoop.mapred.openflow;

public class UndirectedHostPair extends HostPair {
	public UndirectedHostPair() {
        super(false);
    }
    public UndirectedHostPair(String host1, String host2) {
    	super(host1, host2, false);
    }
    public UndirectedHostPair(Integer host1, Integer host2) {
    	super(host1, host2, false);
    }
}
