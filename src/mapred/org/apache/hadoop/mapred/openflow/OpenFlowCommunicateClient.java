package org.apache.hadoop.mapred.openflow;

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

import org.apache.hadoop.io.Writable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

public class OpenFlowCommunicateClient extends Thread {
    static final Log LOG =
        LogFactory.getLog(OpenFlowCommunicateClient.class.getName());

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
    public void sendMRJobInfoToController(MRJobInfo mrJobInfo) throws IOException {
        out.writeInt(HadoopToControllerCommand.MR_JOB_CONTENT.getNum());
        mrJobInfo.write(out);
    }
}
