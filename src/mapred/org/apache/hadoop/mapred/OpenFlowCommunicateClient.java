package org.apache.hadoop.mapred;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.hadoop.io.Writable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

public class OpenFlowCommunicateClient extends Thread {
    static final Log LOG =
        LogFactory.getLog(OpenFlowCommunicateClient.class.getName());

    private static enum HadoopToControllerCommand {
        ERROR(-1), EXIT(0), MR_JOB_CONTENT(1);

        private int commandNum;
        HadoopToControllerCommand(int commandNum) {
            this.commandNum = commandNum;
        }
        public int getCommandNum() {
            return commandNum;
        }
        public static HadoopToControllerCommand getHadoopToControllerCommand(int commandNum) {
            for(HadoopToControllerCommand command : HadoopToControllerCommand.values())
                if(command.getCommandNum() == commandNum)
                    return command;
            return ERROR;
        }
    };
    private static enum ControllerToHadoopCommand {
        ERROR(-1), EXIT(0), TOPO_CONTENT(1);

        private int commandNum;
        ControllerToHadoopCommand(int commandNum) {
            this.commandNum = commandNum;
        }
        public int getCommandNum() {
            return commandNum;
        }
        public static ControllerToHadoopCommand getControllerToHadoopCommand(int commandNum) {
            for(ControllerToHadoopCommand command : ControllerToHadoopCommand.values())
                if(command.getCommandNum() == commandNum)
                    return command;
            return ERROR;
        }
    };

    ////////////
    // Fields //
    ////////////

    private final static int DEFAULT_CONTROLLER_PORT = 5799;
    private final static String DEFAULT_CONTROLLER_IP = "192.168.3.20";

    private int controllerPort;
    private String controllerIP;
    private Socket clientSocket;
    private InetSocketAddress serverAddress;
    private AtomicBoolean isConnected;
    private DataInput in;
    private DataOutput out;

    private AtomicReference<TopologyInfo> topologyInfo;

    /////////////////
    // Constructor //
    /////////////////
    public OpenFlowCommunicateClient(String controllerIP, int controllerPort) {
        this.controllerIP = controllerIP;
        this.controllerPort = controllerPort;
        in = null;
        out = null;

        topologyInfo = new AtomicReference<TopologyInfo>(null);

        clientSocket = new Socket();
        serverAddress = new InetSocketAddress(controllerIP, controllerPort);
        isConnected = new AtomicBoolean(false);
    }
    public OpenFlowCommunicateClient(String controllerIP) {
        this(controllerIP, DEFAULT_CONTROLLER_PORT);
    }
    public OpenFlowCommunicateClient(int controllerPort) {
        this(DEFAULT_CONTROLLER_IP, controllerPort);
    }

    ///////////////////////
    // getter and setter //
    ///////////////////////
    public TopologyInfo getTopologyInfo() {
        TopologyInfo currentTopologyInfo = topologyInfo.getAndSet(null);
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
        if(isConnectedToController())
            keepReceivingMessageFromController();
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
        mainReceivingLoop:
        while(true) {
            ControllerToHadoopCommand receiveCommand = readCommandFromController();

            switch(receiveCommand) {
                case EXIT:
                    break mainReceivingLoop;
                case TOPO_CONTENT:
                    readTopologyInfoFromController();
                    break;
                default:
                    LOG.info("### ControllerToHadoopCommand error in OpenFlowCommunicateClient");
            }
        }
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
    private ControllerToHadoopCommand readCommandFromController() {
        try {
            int receiveCommandNum = in.readInt();
            return ControllerToHadoopCommand.getControllerToHadoopCommand(receiveCommandNum);
        } catch(IOException e) {
            return ControllerToHadoopCommand.ERROR;
        }
    }
    private void readTopologyInfoFromController() {
        try {
            TopologyInfo newTopologyInfo = new TopologyInfo();
            newTopologyInfo.readFields(in);

            topologyInfo.set(newTopologyInfo);
        } catch(IOException e) {
            //do nothing
        }
    }
    public sendMRJobInfoToController() {

    }
}