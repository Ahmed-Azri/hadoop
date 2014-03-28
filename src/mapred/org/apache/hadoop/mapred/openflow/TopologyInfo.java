package org.apache.hadoop.mapred.openflow;

import java.util.Map;
import java.util.HashMap;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

class TopologyInfo {
    public int hostDistanceNumber;
    public Map<HostPair, Integer> hostDistanceList;

    public TopologyInfo() {
        hostDistanceNumber = 0;
        hostDistanceList = new HashMap<HostPair, Integer>();
    }
    public void write(DataOutput out) throws IOException {
        hostDistanceNumber = hostDistanceList.size();

        out.writeInt(hostDistanceNumber);
        for(HostPair hdi : hostDistanceList.keySet()) {
            hdi.write(out);
            out.writeInt(hostDistanceList.get(hdi));
        }
    }
    public void readFields(DataInput in) throws IOException {
        hostDistanceList.clear();

        hostDistanceNumber = in.readInt();
        for(int i=0; i < hostDistanceNumber; ++i) {
            HostPair hdi = new HostPair();
            hdi.readFields(in);
            int dist = in.readInt();
            hostDistanceList.put(hdi, dist);
        }
    }
}