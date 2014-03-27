package org.apache.hadoop.mapred;

import java.util.Map;
import java.util.HashMap;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

class TopologyInfo implements Writable {
    private int hostDistanceNumber;
    private Map<HostDistanceInfo, Integer> hostDistanceList;

    public TopologyInfo() {
        hostDistanceNumber = 0;
        hostDistanceList = new HashMap<HostDistanceInfo, Integer>();
    }
    public void addHostDistanceInfo(Integer host1, Integer host2, int dist) {
        HostDistanceInfo hostDistanceInfo = new HostDistanceInfo(host1, host2);
        hostDistanceList.put(hostDistanceInfo, dist);
        hostDistanceNumber = hostDistanceList.size();
    }
    public int getHostDistanceNumber() {
        return hostDistanceNumber;
    }
    public Map<HostDistanceInfo, Integer> getHostDistanceList() {
        return hostDistanceList;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(hostDistanceNumber);
        for(HostDistanceInfo hdi : hostDistanceList.keySet()) {
            hdi.write(out);
            out.writeInt(hostDistanceList.get(hdi));
        }
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        hostDistanceNumber = in.readInt();
        for(int i=0; i < hostDistanceNumber; ++i) {
            HostDistanceInfo hdi = new HostDistanceInfo();
            hdi.readFields(in);
            int dist = in.readInt();
            hostDistanceList.put(hdi, dist);
        }
    }
}

class HostDistanceInfo implements Writable {
    public Pair<Integer, Integer> hostPair;
    public HostDistanceInfo() {
        this(0, 0);
    }
    public HostDistanceInfo(Integer host1, Integer host2) {
        this(new Pair<Integer, Integer>(host1, host2));
    }
    public HostDistanceInfo(Pair<Integer, Integer> hostPair) {
        this.hostPair = hostPair;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(hostPair.first.intValue());
        out.writeInt(hostPair.second.intValue());
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        hostPair.first = in.readInt();
        hostPair.second = in.readInt();
    }
}
