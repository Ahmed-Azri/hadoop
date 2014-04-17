package org.apache.hadoop.mapred.openflow;

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public class MRJobInfoList {
    public long serialNum;
    public boolean isChange;
    public int mrJobInfoNum;
    public Map<SenderReceiverPair, Integer> mrJobInfo;

    public MRJobInfoList() {
        serialNum = -1;
        isChange = false;
        mrJobInfoNum = 0;
        mrJobInfo = new HashMap<SenderReceiverPair, Integer>();
    }
    public void write(DataOutput out) throws IOException {
        out.writeLong(serialNum);

        mrJobInfoNum = mrJobInfo.size();

        out.writeInt(mrJobInfoNum);
        for(SenderReceiverPair connection : mrJobInfo.keySet()) {
            connection.write(out);
            out.writeInt(mrJobInfo.get(connection).intValue());
        }
    }
    public void readFields(DataInput in) throws IOException {
        serialNum = in.readLong();
        mrJobInfoNum = in.readInt();

        mrJobInfo.clear();
        for(int i=0; i<mrJobInfoNum; ++i) {
            SenderReceiverPair connection = new SenderReceiverPair();
            connection.readFields(in);
            int size = in.readInt();
            mrJobInfo.put(connection, size);
        }
    }
}
