package org.apache.hadoop.mapred.openflow;

public class SenderReceiverPair extends HostPair {
	public SenderReceiverPair() {
        super(true);
    }
    public SenderReceiverPair(String host1, String host2) {
    	super(host1, host2, true);
    }
    public SenderReceiverPair(Integer host1, Integer host2) {
    	super(host1, host2, true);
    }
}
