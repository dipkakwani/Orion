package edu.msu.cse.causalSpartan.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import edu.msu.cse.dkvf.Utils;
import edu.msu.cse.dkvf.ServerConnector.NetworkStatus;
import edu.msu.cse.dkvf.config.Config;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata.RotMessage;
import edu.msu.cse.dkvf.metadata.Metadata.RotReply;
import edu.msu.cse.dkvf.metadata.Metadata.ClientMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ClientReply;

import edu.msu.cse.dkvf.config.ConfigReader.ServerInfo;

import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

public class RotClient implements Runnable{
    int dcId;
    int numOfPartitions;
    LinkedBlockingDeque<RotMessage> rotMessages;
    LinkedBlockingDeque<RotReply> rotReplies;
    ArrayList<ServerInfo> pendingServers;
    Map<String, CodedOutputStream> serversOut = new HashMap<>();
    Map<String, CodedInputStream> serversIn = new HashMap<>();
    Map<String, Socket> sockets = new HashMap<>();
    Config cnf;
    int sleepTime;

    public RotClient(ConfigReader cnfReader, LinkedBlockingDeque<RotMessage> rotMessages,
                     LinkedBlockingDeque<RotReply> rotReplies) {
        HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();
        dcId = new Integer(protocolProperties.get("dc_id").get(0));
        numOfPartitions = new Integer(protocolProperties.get("num_of_partitions").get(0));
        this.rotMessages = rotMessages;
        this.rotReplies = rotReplies;
        this.cnf = cnfReader.getConfig();
        this.sleepTime = new Integer(cnf.getConnectorSleepTime().trim());
        this.pendingServers = cnfReader.getServerInfos();
        // Connect to all servers
        connectToServers();
    }

    private int findPartition(String key) throws NoSuchAlgorithmException {
        long hash = Utils.getMd5HashLong(key);
        return (int) (hash % numOfPartitions);
    }

    @Override
    public void run() {
        RotMessage rm = null;
        while (true) {
            while ((rm = rotMessages.poll()) != null) {
//                protocolLOGGER.finest("Sending rot message.. " + rm.getKey());
                ClientMessage cm = ClientMessage.newBuilder().setRotMessage(rm).build();

                // Contact the partition of the key for ROT
                int partition = 0;
                String serverId = null;
                try {
                    partition = findPartition(rm.getKey());
                    serverId = dcId + "_" + partition;
//                    protocolLOGGER.finest("Server ID: " + serverId);
                    if (sendToServer(serverId, cm) == NetworkStatus.FAILURE) {
//                        protocolLOGGER.severe("Failed to send to server " + serverId);
                        continue;
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                ClientReply cr = readFromServer(serverId);
                if (cr != null && cr.getStatus()) {
//                    protocolLOGGER.finest("Got ROT reply " + rm.getKey());
                    rotReplies.add(cr.getRotReply());
                } else {
//                    protocolLOGGER.severe("Server could not get the ROT key = " + rm.getKey());
                    rotReplies.add(RotReply.newBuilder().setKey(rm.getKey()).setValue(ByteString.EMPTY).build());
                }
            }
        }
    }

    void connectToServers() {
        // Periodically tries to connect to pending servers. Once successfully
        // connected,
        // add them to servers and remove them from pending servers.
        int i = 0;
        Socket newSocket = null;
        while (pendingServers.size() > 0) {
            try {
                newSocket = new Socket(pendingServers.get(i).ip, pendingServers.get(i).port);
                CodedOutputStream out = CodedOutputStream.newInstance(newSocket.getOutputStream());
                serversOut.put(pendingServers.get(i).id, out);
                CodedInputStream in = CodedInputStream.newInstance(newSocket.getInputStream());
                serversIn.put(pendingServers.get(i).id, in);
//                LOGGER.fine(MessageFormat.format("Connected to server with\n\tid= {0} \n\tip= {1} \n\tport= {2}",
//                        pendingServers.get(i).id,
//                        pendingServers.get(i).ip,
//                        String.valueOf(pendingServers.get(i).port)
//                ));
                sockets.put(pendingServers.get(i).id, newSocket);
                pendingServers.remove(i);

            } catch (Exception e) {
                try {
                    if (newSocket != null)
                        newSocket.close();
                } catch (Exception e1) {

                }
            } finally {
                if (pendingServers.size() > 0) {
                    i = i++ % pendingServers.size();
                    if (i == pendingServers.size() - 1)
                        try {
                            Thread.sleep(sleepTime);
                        } catch (InterruptedException e) {
                            System.out.println("Problem in sleeping time in ServerConnector");
                            e.printStackTrace();
                        }
                }
            }
        }
//        LOGGER.info("Sucessfully connected to all servers.");
    }
    /**
     * Sends a client message to the server with the given ID.
     *
     * @param serverId The ID of the destination server.
     * @param cm       The client message to send
     * @return The result of the operation
     */
    public NetworkStatus sendToServer(String serverId, ClientMessage cm) {
        try {
            serversOut.get(serverId).writeInt32NoTag(cm.getSerializedSize());
            cm.writeTo(serversOut.get(serverId));
            serversOut.get(serverId).flush();
            //cm.writeDelimitedTo(serversOut.get(serverId));
//            frameworkLOGGER.finer(MessageFormat.format("Sent to server with id= {0} \n{1}", serverId, cm.toString()));
            return NetworkStatus.SUCCESS;
        } catch (Exception e) {
//            frameworkLOGGER.warning(Utils.exceptionLogMessge(MessageFormat.format("Problem in sending to server with Id= {0}", serverId), e));
            return NetworkStatus.FAILURE;
        }
    }

    /**
     * Reads from input stream of the server with the given ID.
     *
     * @param serverId The ID of the server to read from its input stream.
     * @return The received ClientReply message.
     */
    public ClientReply readFromServer(String serverId) {
        try {
            int size = serversIn.get(serverId).readInt32();
            byte[] result = serversIn.get(serverId).readRawBytes(size);
            return ClientReply.parseFrom(result);
        } catch (Exception e) {
            //debug
//            frameworkLOGGER.warning("serversIn= " + serverId + " serversIn.get(serverId) = " + serversIn.get(serverId) + " serversOut.get(serverId)= " + serversOut.get(serverId));
//            frameworkLOGGER.warning(Utils.exceptionLogMessge(MessageFormat.format("Problem in reading response from server with Id= {0}", serverId), e));
            return null;
        }
    }
}
