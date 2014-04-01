package org.apache.hadoop.mapred.openflow;

import java.util.Map;
import java.util.HashMap;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

public class TopologyInfo {
    public int hostDistanceNumber;
    public Map<UndirectedHostPair, Integer> hostDistanceList;

    public TopologyInfo() {
        hostDistanceNumber = 0;
        hostDistanceList = new HashMap<UndirectedHostPair, Integer>();
    }
    public void write(DataOutput out) throws IOException {
        hostDistanceNumber = hostDistanceList.size();

        out.writeInt(hostDistanceNumber);
        for(UndirectedHostPair hdi : hostDistanceList.keySet()) {
            hdi.write(out);
            out.writeInt(hostDistanceList.get(hdi));
        }
    }
    public void readFields(DataInput in) throws IOException {
        hostDistanceList.clear();

        hostDistanceNumber = in.readInt();
        for(int i=0; i < hostDistanceNumber; ++i) {
            UndirectedHostPair hdi = new UndirectedHostPair();
            hdi.readFields(in);
            int dist = in.readInt();
            hostDistanceList.put(hdi, dist);
        }
    }
}
