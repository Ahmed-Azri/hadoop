package org.apache.hadoop.mapred.openflow;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
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
		public Map<MapReduceJobTask, MapReduceInfo> taskInfo 
							= new HashMap<MapReduceJobTask, MapReduceInfo>();
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

    private Map<Integer, MapReduceJobInfo> mapRecord;
    private Map<Integer, MapReduceJobInfo> reduceRecord;
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

        mapRecord = new HashMap<Integer, MapReduceJobInfo>();
        reduceRecord = new HashMap<Integer, MapReduceJobInfo>();
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
//		LOG.info("### read new topology: " + newTopologyInfo);
		//

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
            out.writeInt(HadoopToControllerCommand.MR_JOB_CONTENT.getNum());
            mrJobInfoList.write(out);
        }
    }

	// **************************
	// Methods provided to hadoop
	// **************************
    public void addMapperInfo(int taskTrackerIPAddress, String jobId, int mapperId) {
        //modify mapRecord
        synchronized(mapRecord) {
			MapReduceJobInfo mapJobInfo = createAndGetJobInfoInRecord(taskTrackerIPAddress, mapRecord);
			MapReduceInfo info = createAndGetMRInfoInJob(jobId, mapperId, mapJobInfo.taskInfo);

/*			LOG.info("in addMapperInfo, taskTracker: " 
					 + InternetUtil.fromIPv4Address(taskTrackerIPAddress) +
					 ", jobId: " + jobId + ", mapperId: " + mapperId);*/
        }
    }
	
    public void addReducerInfo(int taskTrackerIPAddress, String jobId, int reducerId) {
        //modify shuffleRecord
        synchronized(mrJobInfoList) {
			synchronized(reduceRecord) {
				synchronized(mapRecord) {
					Map<SenderReceiverPair, Integer> shuffleRecord = mrJobInfoList.mrJobInfo;
					for(Integer mapper : mapRecord.keySet()) {
						//ignore local case
						if(mapper.equals(taskTrackerIPAddress))
							continue;

						MapReduceJobInfo mapJobInfo = mapRecord.get(mapper);
						for(MapReduceJobTask jobTask : mapJobInfo.taskInfo.keySet()) {
							if(!jobId.equals(jobTask.jobId))
								continue;
							MapReduceInfo mapInfo = mapJobInfo.taskInfo.get(jobTask);
							if(!mapInfo.mapping.containsKey(reducerId))
								continue;

							//record in mrJobInfoList.mrJobInfo
							SenderReceiverPair connection
								= new SenderReceiverPair(mapper, taskTrackerIPAddress);
							int currentBytes = createAndGetSizeInConnection(connection,
																			shuffleRecord).intValue();
							
							int newBytes = mapInfo.mapping.get(reducerId);
							shuffleRecord.put(connection, currentBytes + newBytes);

							//record in reduceRecord
							MapReduceJobInfo reduceJobInfo 
								= createAndGetJobInfoInRecord(taskTrackerIPAddress, reduceRecord);
							MapReduceInfo reduceInfo 
								= createAndGetMRInfoInJob(jobId, reducerId, reduceJobInfo.taskInfo);
							int reduceReceiveBytes 
								= createAndGetSizeInIDPair(mapper, reduceInfo.mapping).intValue();
							reduceInfo.mapping.put(mapper, reduceReceiveBytes + newBytes);
						}
					}
					showMRJobInfoListMessage();
	            }
			}
        }
    }
    public void recordMapInMRTable(int taskTrackerIPAddress, TaskStatus report) {
        //modify mapRecord
        synchronized(mapRecord) {
            String jobId = getJobID(report);
			int mapperId = getTaskID(report);
            Map<Integer, Integer> newMapInfoList = report.getMapReduceInfo();
            if(newMapInfoList == null)
                return;

			long serialNum = report.getSerialNumber();
			MapReduceJobInfo mapJobInfo = mapRecord.get(taskTrackerIPAddress);
			MapReduceJobTask mapJobTask = new MapReduceJobTask(jobId, mapperId);
			MapReduceInfo mapInfo = mapJobInfo.taskInfo.get(mapJobTask);
/*			LOG.info("@@@ in recordMapInMRTable, mapInfo.serialNum: " + mapInfo.serialNum + 
					 ", serialNum: " + serialNum);*/
			if(mapInfo.serialNum == serialNum)
				return;
			mapInfo.serialNum = serialNum;
			for(Integer reducerId : newMapInfoList.keySet()) {
				int receivedBytes = createAndGetSizeInIDPair(reducerId, mapInfo.mapping).intValue();
				int newReceivedBytes = newMapInfoList.get(reducerId).intValue();
				mapInfo.mapping.put(reducerId, receivedBytes + newReceivedBytes);
/*				LOG.info("@@@ in recordMapInMRTable, mapperId: " + mapperId + ", mapper: " + report.getTaskID().toString() + 
						 " to reducerId: " + reducerId + ", serialNum: " + serialNum + ", size: " + newReceivedBytes);*/
			}
			
			//
/*			StringBuffer sb = new StringBuffer();
			sb.append("\n\t### in recordMapInMRTable, dump mapRecord table\n");
			for(Integer mapper : mapRecord.keySet()) {
				sb.append("\t\tmapper: " + InternetUtil.fromIPv4Address(mapper) + "\n");
				MapReduceJobInfo jobInfo = mapRecord.get(mapper);
				for(MapReduceJobTask jobTask : jobInfo.taskInfo.keySet()) {
					sb.append("\t\t\tjobId: " + jobTask.jobId + ", mapperId: " + jobTask.taskId + "\n");
					MapReduceInfo info = jobInfo.taskInfo.get(jobTask);
					for(Integer reducerId : info.mapping.keySet()) {
						sb.append("\t\t\t\tReducerId: " + reducerId + ", size: " + info.mapping.get(reducerId) + "\n");
					}
				}
			}
			LOG.info(sb.toString());*/
			//
        }
    }
    public void recordShuffleInMRTable(int taskTrackerIPAddress, TaskStatus report) {
        synchronized(mrJobInfoList) {
			synchronized(reduceRecord) {
	            String jobId = getJobID(report);
				int reducerId = getTaskID(report);
		        Map<Integer, Integer> newMapInfoList = report.getMapReduceInfo();
		        if(newMapInfoList == null)
		            return;

				long serialNum = report.getSerialNumber();
				MapReduceJobInfo reduceJobInfo = reduceRecord.get(taskTrackerIPAddress);
				MapReduceJobTask reduceJobTask = new MapReduceJobTask(jobId, reducerId);
				MapReduceInfo reduceInfo = reduceJobInfo.taskInfo.get(reduceJobTask);

				//
/*				LOG.info("### in recordShuffleInMRTable, task tracker: " + report.getTaskID().toString() +
						", jobId: " + jobId + ", reducerId: " + reducerId + 
						", reducerInfo.serialNum: " + reduceInfo.serialNum + ", seralNum: " + serialNum);*/
				//
				if(reduceInfo.serialNum == serialNum)
					return;
				reduceInfo.serialNum = serialNum;
				Map<SenderReceiverPair, Integer> shuffleRecord = mrJobInfoList.mrJobInfo;
				for(Integer mapper : newMapInfoList.keySet()) {
					if(!reduceInfo.mapping.containsKey(mapper) 
					   || reduceInfo.mapping.get(mapper) == null)
						continue;


					int reduceShuffleBytes = reduceInfo.mapping.get(mapper).intValue();
					int receivedBytes = newMapInfoList.get(mapper).intValue();
					
/*					LOG.info("### in reduceRecord, receive " + receivedBytes + 
							" from " + InternetUtil.fromIPv4Address(mapper) +
							", we have " + reduceShuffleBytes + " bytes, leave " + (reduceShuffleBytes - receivedBytes));*/
					reduceShuffleBytes -= receivedBytes;
					if(reduceShuffleBytes <=0)
						reduceInfo.mapping.remove(mapper);
					else
						reduceInfo.mapping.put(mapper, reduceShuffleBytes);
				
					SenderReceiverPair connection = new SenderReceiverPair(mapper, taskTrackerIPAddress);
					if(!shuffleRecord.containsKey(connection) 
					   || shuffleRecord.get(connection) == null) //should not happen...
					{
/*						LOG.info("### we have no connection [" + InternetUtil.fromIPv4Address(connection.getFirstHost()) +
								", " + InternetUtil.fromIPv4Address(connection.getSecondHost()) + "] in mrJobInfoList");*/
						continue;
					}
					int needToTransmissionBytes = shuffleRecord.get(connection).intValue();
/*					LOG.info("### in mrJobInfo, needToTransmissionBytes: " + needToTransmissionBytes + 
							 ", leave " + (needToTransmissionBytes - receivedBytes));*/

					needToTransmissionBytes -= receivedBytes;
					if(needToTransmissionBytes <=0)
						shuffleRecord.remove(connection);
					else
						shuffleRecord.put(connection, needToTransmissionBytes);
			        if(!mrJobInfoList.isChange)
			            mrJobInfoList.serialNum += 1;
			        mrJobInfoList.isChange = true;
				}
//				showMRJobInfoListMessage();
			}
        }
    }
    public void cleanMapReduceFromMRTable(TaskStatus report) {
		synchronized(reduceRecord) {
			synchronized(mapRecord) {
				String jobId = getJobID(report);
				cleanupJobInRecord(mapRecord.values(), jobId);
				cleanupJobInRecord(reduceRecord.values(), jobId);
				LOG.info("in cleanMapReduceFromMRTable, clean job " + jobId);
			}
		}
    }
	private String getJobID(TaskStatus task) {
        TaskAttemptID taskId = task.getTaskID();
        return taskId.getJobID().toString();
    }
	private int getTaskID(TaskStatus task) {
        TaskAttemptID taskId = task.getTaskID();
		return taskId.getTaskID().getId();
	}
	private MapReduceJobInfo createAndGetJobInfoInRecord(Integer key, 
														 Map<Integer, MapReduceJobInfo> table) {
		if(!table.containsKey(key)) {
			table.put(key, new MapReduceJobInfo());
		}
		return table.get(key);
	}
	private MapReduceInfo createAndGetMRInfoInJob(String jobId, int taskId, 
												  Map<MapReduceJobTask, MapReduceInfo> table) {
		MapReduceJobTask key = new MapReduceJobTask(jobId, taskId);
		if(!table.containsKey(key))
			table.put(key, new MapReduceInfo());
		MapReduceInfo mapReduceInfo = table.get(key);
		return table.get(key);
	}
	private Integer createAndGetSizeInIDPair(Integer key, Map<Integer, Integer> table) {
		if(!table.containsKey(key))
			table.put(key, new Integer(0));
		return table.get(key);
	}
	private Integer createAndGetSizeInConnection(SenderReceiverPair key,
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
				"isChange: " + mrJobInfoList.isChange + "\n");
		for(SenderReceiverPair connection : mrJobInfoList.mrJobInfo.keySet()) {
			sb.append("\t\tSender: " + InternetUtil.fromIPv4Address(connection.getFirstHost()) + 
					  ", Receiver:" + InternetUtil.fromIPv4Address(connection.getSecondHost()) + 
					  ", size: " + mrJobInfoList.mrJobInfo.get(connection) + "\n");
		}
		LOG.info(sb.toString());
	}
}
