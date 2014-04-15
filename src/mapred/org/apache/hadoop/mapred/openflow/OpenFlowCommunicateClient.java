package org.apache.hadoop.mapred.openflow;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.EOFException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

public class OpenFlowCommunicateClient extends Thread {
    static final Log LOG =
        LogFactory.getLog(OpenFlowCommunicateClient.class.getName());

    class MapReduceInfo {
        public Map<Integer, Integer> mapping = new HashMap<Integer, Integer>();
    }
    class MapReduceLocation {
        public Map<String, MapReduceInfo> outputLocation = new HashMap<String, MapReduceInfo>();
    }

    ////////////
    // Fields //
    ////////////
    private int controllerPort;
    private String controllerIP;
    private Socket clientSocket;
    private InetSocketAddress serverAddress;
    private AtomicBoolean isConnected;
    private DataInput in;
    private DataOutput out;

    private TopologyInfo topologyInfo;

    private Map<Integer, MapReduceLocation> mapRecord;
    private MRJobInfoList mrJobInfoList;

    /////////////////
    // Constructor //
    /////////////////
    public OpenFlowCommunicateClient(String controllerIP, int controllerPort) {
        this.controllerIP = controllerIP;
        this.controllerPort = controllerPort;
        in = null;
        out = null;

        topologyInfo = null;

        clientSocket = new Socket();
        serverAddress = new InetSocketAddress(controllerIP, controllerPort);
        isConnected = new AtomicBoolean(false);

        mapRecord = new HashMap<Integer, MapReduceLocation>();
        mrJobInfoList = new MRJobInfoList();

		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		scheduler.scheduleAtFixedRate(new SendTask(), 1000, 500, TimeUnit.MILLISECONDS);
    }

    ///////////////////////
    // getter and setter //
    ///////////////////////
    public TopologyInfo getTopologyInfo() {
        TopologyInfo currentTopologyInfo = topologyInfo;
        return currentTopologyInfo;
    }
    public boolean isConnectedToController() {
        return isConnected.get();
    }
    public int getControllerPort() {
        return controllerPort;
    }
    public String getControllerIP() {
        return controllerIP;
    }

    /////////////////
    // Main method //
    /////////////////
    @Override
    public void run() {
        //TODO: need to process close state
        tryConnectToController();
        if(isConnectedToController()) {
            keepReceivingMessageFromController();
            closeConnection();
        }
    }
    private void tryConnectToController() {
        while(true) {
            try {
                clientSocket.connect(serverAddress);
                in = new DataInputStream(clientSocket.getInputStream());
                out = new DataOutputStream(clientSocket.getOutputStream());
                isConnected.set(true);
                break;
            } catch(IOException e) {
                LOG.error("### IOException in OpenFlowCommunicateClient :" + e.toString());
                in = null;
                out = null;
                isConnected.set(false);
            } catch(Exception e) {
                LOG.error("### Exception in OpenFlowCommunicateClient : " + e.toString());
                in = null;
                out = null;
                isConnected.set(false);
                break;
            }

            try {
                Thread.sleep(500);
            } catch(InterruptedException e) {
                //do nothing
            }
        }
    }
    private void keepReceivingMessageFromController() {
        LOG.info("### connection setup, keep receiving message from controller");
        mainReceivingLoop:
        while(true) {
            try {
                ControllerToHadoopCommand receiveCommand = readCommandFromController();

                switch(receiveCommand) {
                    case EXIT:
                    case EOF:
                        break mainReceivingLoop;
                    case TOPO_CONTENT:
                        readTopologyInfoFromController();
                        break;
                    default:
                        LOG.info("### ControllerToHadoopCommand error in OpenFlowCommunicateClient");
                }
            } catch(IOException e) {
                LOG.error("IOException in openflow client", e);
                break;
            }
        }
    }
    private void closeConnection() {
        try {
            clientSocket.close();
        } catch(IOException e) {
            LOG.error(StringUtils.stringifyException(e));
        } finally {
            isConnected.set(false);
            clientSocket = null;
            in = null;
            out = null;
        }
    }
    private ControllerToHadoopCommand readCommandFromController() throws IOException {
        try {
            int receiveCommandNum = in.readInt();
            return ControllerToHadoopCommand.lookup(receiveCommandNum);
        } catch(EOFException e) {
            return ControllerToHadoopCommand.EOF;
        } catch(SocketException e) {
			return ControllerToHadoopCommand.EOF;
		}
    }
    private void readTopologyInfoFromController() throws IOException {
        TopologyInfo newTopologyInfo = new TopologyInfo();
        newTopologyInfo.readFields(in);

		//###
		LOG.info("### read new topology: " + newTopologyInfo);
		//

        topologyInfo =  newTopologyInfo;
    }

	// ***********
	// Send Thread
	// ***********
	class SendTask implements Runnable {
		@Override
		public void run() {
			if(isConnected.get())
				sendMRJobInfoToController();
		}
	}
    private void sendMRJobInfoToController() throws IOException {
        synchronized(mrJobInfoList) {
            out.writeInt(HadoopToControllerCommand.MR_JOB_CONTENT.getNum());
            mrJobInfoList.write(out);
        }
    }

	// **************************
	// Methods provided to hadoop
	// **************************
    public void addMapperInfo(int taskTrackerIPAddress, String jobId) {
        //modify mapRecord
        synchronized(mapRecord) {
            if(!mapRecord.containsKey(taskTrackerIPAddress) || mapRecord.get(taskTrackerIPAddress) == null)
                mapRecord.put(taskTrackerIPAddress, new MapReduceLocation());
            MapReduceLocation mapLocation = mapRecord.get(taskTrackerIPAddress);
            if(!mapLocation.outputLocation.containsKey(jobId) || mapLocation.outputLocation.get(jobId) == null)
                mapLocation.outputLocation.put(jobId, new MapReduceInfo());
        }
    }
    public void addReducerInfo(int taskTrackerIPAddress, String jobId, int reducerId) {
        //modify shuffleRecord
        synchronized(mrJobInfoList) {
            synchronized(mapRecord) {
                Map<SenderReceiverPair, Integer> shuffleRecord = mrJobInfoList.mrJobInfo;
                for(Integer mapper : mapRecord.keySet()) {
                    MapReduceLocation mapReduceLocation = mapRecord.get(mapper);
                    if(!mapReduceLocation.outputLocation.containsKey(jobId))
                        continue;
                    MapReduceInfo mapReduceInfo = mapReduceLocation.outputLocation.get(jobId);
                    if(!mapReduceInfo.mapping.containsKey(reducerId))
                        continue;

                    SenderReceiverPair connection = new SenderReceiverPair(mapper, taskTrackerIPAddress);
                    if(!shuffleRecord.containsKey(connection) || shuffleRecord.get(connection) == null)
                        shuffleRecord.put(connection, new Integer(0));
                    int currentBytes = shuffleRecord.get(connection);
                    int newBytes = mapReduceInfo.mapping.get(reducerId);
                    shuffleRecord.put(connection, currentBytes + newBytes);

                    if(!mrJobInfoList.isChange)
                        mrJobInfoList.serialNum += 1;
                    mrJobInfoList.isChange = true;
                }
            }
        }
    }
    public void recordMapInMRTable(int taskTrackerIPAddress, TaskStatus report) {
        //modify mapRecord
        synchronized(mapRecord) {
            String jobId = getJobID(report);
            Map<Integer, Integer> newMapInfoList = report.getMapReduceInfo();
            if(newMapInfoList == null)
                return;

            MapReduceLocation mapLocation = mapRecord.get(taskTrackerIPAddress);
            MapReduceInfo mapInfo = mapLocation.outputLocation.get(jobId);
            for(Integer reducerId : newMapInfoList.keySet()) {
                if(!mapInfo.mapping.containsKey(reducerId) || mapInfo.mapping.get(reducerId) == null)
                    mapInfo.mapping.put(reducerId, new Integer(0));
                Integer receivedBytes = mapInfo.mapping.get(reducerId);
                Integer newReceivedBytes = new Integer(receivedBytes.intValue() 
                                                    + newMapInfoList.get(reducerId).intValue());
                mapInfo.mapping.put(reducerId, newReceivedBytes);
            }
        }
    }
    public void recordShuffleInMRTable(int taskTrackerIPAddress, TaskStatus report) {
        //modify shuffleRecord
        synchronized(mrJobInfoList) {
            String jobId = getJobID(report);
            Map<Integer, Integer> newMapInfoList = report.getMapReduceInfo();
            if(newMapInfoList == null)
                return;

            Map<SenderReceiverPair, Integer> shuffleRecord = mrJobInfoList.mrJobInfo;
            for(Integer mapper : newMapInfoList.keySet()) {
                SenderReceiverPair connection = new SenderReceiverPair(mapper, taskTrackerIPAddress);
                int transmissionBytes = shuffleRecord.get(connection).intValue();
                transmissionBytes -= newMapInfoList.get(mapper).intValue();
                if(transmissionBytes <= 0)
                    shuffleRecord.remove(connection);
                else
                    shuffleRecord.put(connection, transmissionBytes);
                if(!mrJobInfoList.isChange)
                    mrJobInfoList.serialNum += 1;
                mrJobInfoList.isChange = true;
            }
        }
    }
    public void cleanMapReduceFromMRTable(TaskStatus report) {
        //modify mapRecord
        synchronized(mapRecord) {
            String jobId = getJobID(report);
            for(Integer mapper : mapRecord.keySet()) {
				MapReduceLocation mapLocation = mapRecord.get(mapper);
                if(mapLocation.outputLocation.containsKey(jobId))
                    mapLocation.outputLocation.remove(jobId);
            }
        }
    }
    private String getJobID(TaskStatus task) {
        TaskAttemptID taskId = task.getTaskID();
        return taskId.getJobID().toString();
    }
}
