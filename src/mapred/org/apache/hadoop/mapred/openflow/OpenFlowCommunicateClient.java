package org.apache.hadoop.mapred.openflow;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
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

    protected static Map<Integer, Integer> SIM_IP_TO_REAL_IP;
    protected static Map<Integer, Integer> REAL_IP_TO_SIM_IP;
    static {
        SIM_IP_TO_REAL_IP = new HashMap<Integer, Integer>();
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.1"), InternetUtil.toIPv4Address("192.168.2.1"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.2"), InternetUtil.toIPv4Address("192.168.2.2"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.3"), InternetUtil.toIPv4Address("192.168.2.3"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.4"), InternetUtil.toIPv4Address("192.168.2.4"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.5"), InternetUtil.toIPv4Address("192.168.2.5"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.6"), InternetUtil.toIPv4Address("192.168.2.6"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.7"), InternetUtil.toIPv4Address("192.168.2.7"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.9"), InternetUtil.toIPv4Address("192.168.2.8"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.10"), InternetUtil.toIPv4Address("192.168.2.9"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.11"), InternetUtil.toIPv4Address("192.168.2.10"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.12"), InternetUtil.toIPv4Address("192.168.2.11"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.13"), InternetUtil.toIPv4Address("192.168.2.12"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.14"), InternetUtil.toIPv4Address("192.168.2.13"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.15"), InternetUtil.toIPv4Address("192.168.2.14"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.16"), InternetUtil.toIPv4Address("192.168.2.15"));
        SIM_IP_TO_REAL_IP.put(InternetUtil.toIPv4Address("1.0.1.17"), InternetUtil.toIPv4Address("192.168.2.16"));

        REAL_IP_TO_SIM_IP = new HashMap<Integer, Integer>();
        for(Integer simIP : SIM_IP_TO_REAL_IP.keySet())
            REAL_IP_TO_SIM_IP.put(SIM_IP_TO_REAL_IP.get(simIP), simIP);
    }


	class MapReduceInfo {
		long serialNum = -1;
		public Map<Integer, Integer> mapping = new HashMap<Integer, Integer>();
	}
	class MapReduceJobTask implements Comparable<MapReduceJobTask> {
		public String jobId;
		public int taskId;
		public MapReduceJobTask(String job, int task) {
			jobId = job;
			taskId = task;
		}
		@Override
		public int compareTo(MapReduceJobTask o) {
			if(jobId == null && o.jobId != null)
				return -1;
			if(jobId != null && o.jobId == null)
				return 1;
			int cmp;
			if(jobId == null && o.jobId == null)
				cmp = 0;
			else
				cmp = jobId.compareTo(o.jobId);
			return cmp != 0? cmp : Integer.valueOf(taskId).compareTo(o.taskId);
		}
		@Override
		public int hashCode() {
			return jobId != null? (31*jobId.hashCode() + taskId) : taskId;
		}
		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof MapReduceJobTask))
				return false;
			if(this == obj)
				return true;
			MapReduceJobTask task = (MapReduceJobTask)obj;
			if(jobId == null) {
				if(task.jobId == null)
					return taskId == task.taskId;
				else
					return false;
			}
			return jobId.equals(task.jobId) && (taskId == task.taskId);
		}
	}
	class MapReduceJobInfo {
		public ConcurrentMap<MapReduceJobTask, MapReduceInfo> taskInfo
			= new ConcurrentHashMap<MapReduceJobTask, MapReduceInfo>();
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

	private ConcurrentMap<Integer, MapReduceJobInfo> mapRecord;
	private ConcurrentMap<Integer, MapReduceJobInfo> reduceRecord;
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

        mapRecord = new ConcurrentHashMap<Integer, MapReduceJobInfo>();
        reduceRecord = new ConcurrentHashMap<Integer, MapReduceJobInfo>();
        mrJobInfoList = new MRJobInfoList();

		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		scheduler.scheduleAtFixedRate(new SendTask(), 1000, 250, TimeUnit.MILLISECONDS);
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
//                LOG.error("### IOException in OpenFlowCommunicateClient :" + e.toString());
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
        topologyInfo =  newTopologyInfo;
    }

	// ***********
	// Send Thread
	// ***********
	class SendTask implements Runnable {
		@Override
		public void run() {
			try {
				if(isConnected.get())
					sendMRJobInfoToController();
			} catch(IOException e) {
			}
		}
	}
    private void sendMRJobInfoToController() throws IOException {
        synchronized(mrJobInfoList) {
			if(mrJobInfoList.isChange) {
				out.writeInt(HadoopToControllerCommand.MR_JOB_CONTENT.getNum());
				mrJobInfoList.write(out);
				mrJobInfoList.isChange = false;
			}
        }
    }

	// **************************
	// Methods provided to hadoop
	// **************************
    public void addMapperInfo(int taskTrackerIPAddress, String jobId, int mapperId) {
		Integer simTaskTrackerIPAddress = REAL_IP_TO_SIM_IP.get(taskTrackerIPAddress);
		MapReduceJobInfo mapJobInfo = getMRJobInfoInTable(simTaskTrackerIPAddress, mapRecord);
		getMRInfoInTable(jobId, mapperId, mapJobInfo.taskInfo);
    }
	
    public void addReducerInfo(int taskTrackerIPAddress, String jobId, int reducerId) {
		Integer simTaskTrackerIPAddress = REAL_IP_TO_SIM_IP.get(taskTrackerIPAddress);
		for(Map.Entry<Integer, MapReduceJobInfo> mapRecordEntry : mapRecord.entrySet()) {
			Integer mapper = mapRecordEntry.getKey();
			if(mapper.equals(simTaskTrackerIPAddress))
				continue;

			MapReduceJobInfo mapJobInfo = mapRecordEntry.getValue();
			for(Map.Entry<MapReduceJobTask, MapReduceInfo> mapJobInfoEntry : mapJobInfo.taskInfo.entrySet()) {
				MapReduceJobTask jobTask = mapJobInfoEntry.getKey();
				if(!jobId.equals(jobTask.jobId))
					continue;
				MapReduceInfo mapInfo = mapJobInfoEntry.getValue();
				synchronized(mapInfo) {
					if(!mapInfo.mapping.containsKey(reducerId)) {
						continue;
					}

					int newBytes = mapInfo.mapping.get(reducerId);

					//modify reduce record
					MapReduceJobInfo reduceJobInfo = getMRJobInfoInTable(simTaskTrackerIPAddress, reduceRecord);
					MapReduceInfo reduceInfo = getMRInfoInTable(jobId, reducerId, reduceJobInfo.taskInfo);
					synchronized(reduceInfo) {
						int reduceReceiveBytes = getSizeInIDPair(mapper, reduceInfo.mapping).intValue();
						reduceInfo.mapping.put(mapper, reduceReceiveBytes + newBytes);
					}

					//modify mrJobInfoList
					synchronized(mrJobInfoList) {
						Map<SenderReceiverPair, Integer> shuffleRecord = mrJobInfoList.mrJobInfo;
						SenderReceiverPair connection
							= new SenderReceiverPair(mapper, simTaskTrackerIPAddress);
						int currentBytes = getSizeInConnection(connection, shuffleRecord).intValue();	
						shuffleRecord.put(connection, currentBytes + newBytes);
					    if(!mrJobInfoList.isChange)
					        mrJobInfoList.serialNum += 1;
					    mrJobInfoList.isChange = true;
						showMRJobInfoListMessage();
					}
				}
			}
		}
    }
    public void recordMapInMRTable(int taskTrackerIPAddress, TaskStatus report) {
		Integer simTaskTrackerIPAddress = REAL_IP_TO_SIM_IP.get(taskTrackerIPAddress);
        String jobId = getJobID(report);
		int mapperId = getTaskID(report);
        Map<Integer, Integer> newMapInfoList = report.getMapReduceInfo();
        if(newMapInfoList == null)
			return;

		long serialNum = report.getSerialNumber();
		MapReduceJobInfo mapJobInfo = mapRecord.get(simTaskTrackerIPAddress);
		MapReduceJobTask mapJobTask = new MapReduceJobTask(jobId, mapperId);
		MapReduceInfo mapInfo = mapJobInfo.taskInfo.get(mapJobTask);

		synchronized(mapInfo) {
			if(mapInfo.serialNum == serialNum)
				return;
			mapInfo.serialNum = serialNum;
			for(Integer reducerId : newMapInfoList.keySet()) {
				int receivedBytes = getSizeInIDPair(reducerId, mapInfo.mapping).intValue();
				int newReceivedBytes = newMapInfoList.get(reducerId).intValue();
				mapInfo.mapping.put(reducerId, receivedBytes + newReceivedBytes);
			}
		}
    }
    public void recordShuffleInMRTable(int taskTrackerIPAddress, TaskStatus report) {
		Integer simTaskTrackerIPAddress = REAL_IP_TO_SIM_IP.get(taskTrackerIPAddress);
		String jobId = getJobID(report);
		int reducerId = getTaskID(report);
		Map<Integer, Integer> newMapInfoList = report.getMapReduceInfo();
		if(newMapInfoList == null)
			return;

		long serialNum = report.getSerialNumber();
		MapReduceJobInfo reduceJobInfo = reduceRecord.get(simTaskTrackerIPAddress);
		MapReduceJobTask reduceJobTask = new MapReduceJobTask(jobId, reducerId);
		MapReduceInfo reduceInfo = reduceJobInfo.taskInfo.get(reduceJobTask);
		if(reduceInfo == null) {
			LOG.info("### taskTracker: " + InternetUtil.fromIPv4Address(taskTrackerIPAddress) + ", no info about (" + jobId + ", " + reducerId + ")");
			return;
		}

		synchronized(reduceInfo) {
			if(reduceInfo.serialNum == serialNum)
				return;

			reduceInfo.serialNum = serialNum;
			for(Integer realWorldMapper : newMapInfoList.keySet()) {
				Integer mapper = REAL_IP_TO_SIM_IP.get(realWorldMapper);
				if(!reduceInfo.mapping.containsKey(mapper) 
				   || reduceInfo.mapping.get(mapper) == null)
					continue;

				int reduceShuffleBytes = reduceInfo.mapping.get(mapper).intValue();
				int receivedBytes = newMapInfoList.get(realWorldMapper).intValue();

				reduceShuffleBytes -= receivedBytes;
				if(reduceShuffleBytes <=0)
					reduceInfo.mapping.remove(mapper);
				else
					reduceInfo.mapping.put(mapper, reduceShuffleBytes);
	
				synchronized(mrJobInfoList) {
					Map<SenderReceiverPair, Integer> shuffleRecord = mrJobInfoList.mrJobInfo;
					SenderReceiverPair connection = new SenderReceiverPair(mapper, simTaskTrackerIPAddress);
					if(!shuffleRecord.containsKey(connection) 
					   || shuffleRecord.get(connection) == null) { //should not happen...
						continue;
					}
					int needToTransmissionBytes = shuffleRecord.get(connection).intValue();

					needToTransmissionBytes -= receivedBytes;
					if(needToTransmissionBytes <=0)
						shuffleRecord.remove(connection);
					else
						shuffleRecord.put(connection, needToTransmissionBytes);
				    if(!mrJobInfoList.isChange)
				        mrJobInfoList.serialNum += 1;
				    mrJobInfoList.isChange = true;
					showMRJobInfoListMessage();
				}
			}
		}
    }
	public void recordReduce(int taskTrackerIPAddress, TaskStatus report) {
		Integer simTaskTrackerIPAddress = REAL_IP_TO_SIM_IP.get(taskTrackerIPAddress);
		String jobId = getJobID(report);
		int reducerId = getTaskID(report);
		Map<Integer, Integer> newMapInfoList = report.getMapReduceInfo();
		if(newMapInfoList == null)
			return;

		long serialNum = report.getSerialNumber();
		MapReduceJobInfo reduceJobInfo = reduceRecord.get(simTaskTrackerIPAddress);
		MapReduceJobTask reduceJobTask = new MapReduceJobTask(jobId, reducerId);
		MapReduceInfo reduceInfo = reduceJobInfo.taskInfo.get(reduceJobTask);

		synchronized(reduceInfo) {
			for(Integer realWorldMapper : newMapInfoList.keySet()) {
				Integer mapper = REAL_IP_TO_SIM_IP.get(realWorldMapper);
				if(!reduceInfo.mapping.containsKey(mapper) 
				   || reduceInfo.mapping.get(mapper) == null)
					continue;
				//clean up
				int reduceShuffleBytes = reduceInfo.mapping.get(mapper).intValue();
				reduceInfo.mapping.remove(mapper);
				synchronized(mrJobInfoList) {
					Map<SenderReceiverPair, Integer> shuffleRecord = mrJobInfoList.mrJobInfo;
					SenderReceiverPair connection = new SenderReceiverPair(mapper, simTaskTrackerIPAddress);
					if(!shuffleRecord.containsKey(connection) 
					   || shuffleRecord.get(connection) == null) //should not happen...
						continue;
					int needToTransmissionBytes = shuffleRecord.get(connection).intValue();
					needToTransmissionBytes -= reduceShuffleBytes;
					if(needToTransmissionBytes <=0)
						shuffleRecord.remove(connection);
					else
						shuffleRecord.put(connection, needToTransmissionBytes);
				    if(!mrJobInfoList.isChange)
				        mrJobInfoList.serialNum += 1;
				    mrJobInfoList.isChange = true;
				}
			}
		}
	}
    public void cleanMapReduceFromMRTable(TaskStatus report) {
		String jobId = getJobID(report);
		cleanupJobInRecord(mapRecord.values(), jobId);
		cleanupJobInRecord(reduceRecord.values(), jobId);
    }
	private String getJobID(TaskStatus task) {
        TaskAttemptID taskId = task.getTaskID();
        return taskId.getJobID().toString();
    }
	private int getTaskID(TaskStatus task) {
        TaskAttemptID taskId = task.getTaskID();
		return taskId.getTaskID().getId();
	}
	private MapReduceJobInfo getMRJobInfoInTable(Integer key,
												 ConcurrentMap<Integer, MapReduceJobInfo> table) {
		table.putIfAbsent(key, new MapReduceJobInfo());
		return table.get(key);
	}
	private MapReduceInfo getMRInfoInTable(String jobId, int taskId,
										   ConcurrentMap<MapReduceJobTask, MapReduceInfo> table) {
		return getMRInfoInTable(new MapReduceJobTask(jobId, taskId), table);
	}
	private MapReduceInfo getMRInfoInTable(MapReduceJobTask key,
										   ConcurrentMap<MapReduceJobTask, MapReduceInfo> table) {
		table.putIfAbsent(key, new MapReduceInfo());
		return table.get(key);
	}
	private Integer getSizeInIDPair(Integer key, Map<Integer, Integer> table) {
		if(!table.containsKey(key))
			table.put(key, new Integer(0));
		return table.get(key);
	}
	private Integer getSizeInConnection(SenderReceiverPair key,
										Map<SenderReceiverPair, Integer> table) {
		if(!table.containsKey(key))
			table.put(key, new Integer(0));
		return table.get(key);
	}
	private void cleanupJobInRecord(Collection<MapReduceJobInfo> jobInfoSet, String jobId) {
		for(MapReduceJobInfo jobInfo : jobInfoSet) {
			Set<MapReduceJobTask> taskToRemove = new HashSet<MapReduceJobTask>();
			for(MapReduceJobTask jobTask : jobInfo.taskInfo.keySet()) {
				if(jobId.equals(jobTask.jobId))
					taskToRemove.add(jobTask);
			}
			for(MapReduceJobTask jobTask : taskToRemove)
				jobInfo.taskInfo.remove(jobTask);
		}
	}
	private void showMRJobInfoListMessage() {
		StringBuffer sb = new StringBuffer();
		sb.append("\n\tMRJobInfoList, serialNum: " + mrJobInfoList.serialNum + 
				" isChange: " + mrJobInfoList.isChange + "\n");
		for(SenderReceiverPair connection : mrJobInfoList.mrJobInfo.keySet()) {
			sb.append("\t\tSrc: " + InternetUtil.fromIPv4Address(connection.getFirstHost()) + 
					  ", Dst:" + InternetUtil.fromIPv4Address(connection.getSecondHost()) + 
					  ", size: " + mrJobInfoList.mrJobInfo.get(connection) + "\n");
		}
		LOG.info(sb.toString());
	}
}
