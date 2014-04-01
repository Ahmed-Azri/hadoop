package org.apache.hadoop.mapred.openflow;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public abstract class HostPair {
    private Pair<Integer, Integer> hostPair;
    public HostPair(boolean isDirected) {
        this(0, 0, isDirected);
    }
    public HostPair(String host1, String host2, boolean isDirected) {
		this(IPv4AddressConverter.toIPv4Address(host1), 
			 IPv4AddressConverter.toIPv4Address(host2),
             isDirected);
    }
    public HostPair(Integer host1, Integer host2, boolean isDirected) {
        if(isDirected)
            this.hostPair = new Pair<Integer, Integer>(host1, host2);
        else
            this.hostPair = new UndirectedPair<Integer, Integer>(host1, host2);
    }
    public HostPair(Pair<Integer, Integer> hostPair) {
        this.hostPair = hostPair;
    }
    public Pair<Integer, Integer> getHostPairInt() {
        return hostPair;
    }
	public Integer getFirstHost() {
		return hostPair.first;
	}
	public Integer getSecondHost() {
		return hostPair.second;
	}
    public String getFirstHostString() {
        return IPv4AddressConverter.fromIPv4Address(hostPair.first.intValue());
    }
    public String getSecondHostString() {
        return IPv4AddressConverter.fromIPv4Address(hostPair.second.intValue());
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(hostPair.first.intValue());
        out.writeInt(hostPair.second.intValue());
    }
    public void readFields(DataInput in) throws IOException {
        hostPair.first = in.readInt();
        hostPair.second = in.readInt();
    }
}

//all copy from net.floodlightcontroller.packet.IPv4
class IPv4AddressConverter {
    public static String fromIPv4Address(int ipAddress) {
        StringBuffer sb = new StringBuffer();
        int result = 0;
        for (int i = 0; i < 4; ++i) {
            result = (ipAddress >> ((3-i)*8)) & 0xff;
            sb.append(Integer.valueOf(result).toString());
            if (i != 3)
                sb.append(".");
        }
        return sb.toString();
    }
    public static int toIPv4Address(String ipAddress) {
        if (ipAddress == null)
            throw new IllegalArgumentException("Specified IPv4 address must" +
                "contain 4 sets of numerical digits separated by periods");
        String[] octets = ipAddress.split("\\.");
        if (octets.length != 4)
            throw new IllegalArgumentException("Specified IPv4 address must" +
                "contain 4 sets of numerical digits separated by periods");

        int result = 0;
        for (int i = 0; i < 4; ++i) {
            int oct = Integer.valueOf(octets[i]);
            if (oct > 255 || oct < 0)
                throw new IllegalArgumentException("Octet values in specified" +
                        " IPv4 address must be 0 <= value <= 255");
            result |=  oct << ((3-i)*8);
        }
        return result;
    }
}
