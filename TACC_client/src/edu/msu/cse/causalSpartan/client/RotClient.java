package edu.msu.cse.causalSpartan.client;

import com.google.protobuf.ByteString;
import edu.msu.cse.dkvf.DKVFBase;
import edu.msu.cse.dkvf.Utils;
import edu.msu.cse.dkvf.ServerConnector.NetworkStatus;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata.RotMessage;
import edu.msu.cse.dkvf.metadata.Metadata.RotReply;
import edu.msu.cse.dkvf.metadata.Metadata.ClientMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ClientReply;

import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

public class RotClient extends DKVFBase implements Runnable{
    int dcId;
    int numOfPartitions;
    LinkedBlockingDeque<RotMessage> rotMessages;
    LinkedBlockingDeque<RotReply> rotReplies;

    public RotClient(ConfigReader cnfReader, LinkedBlockingDeque<RotMessage> rotMessages,
                     LinkedBlockingDeque<RotReply> rotReplies) {
        super(cnfReader);
        HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();
        dcId = new Integer(protocolProperties.get("dc_id").get(0));
        numOfPartitions = new Integer(protocolProperties.get("num_of_partitions").get(0));
        this.rotMessages = rotMessages;
        this.rotReplies = rotReplies;
        // Connect to all servers
        this.connectToServers();
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
                protocolLOGGER.finest("Sending rot message.. " + rm.getKey());
                ClientMessage cm = ClientMessage.newBuilder().setRotMessage(rm).build();

                // Contact the partition of the key for ROT
                int partition = 0;
                String serverId = null;
                try {
                    partition = findPartition(rm.getKey());
                    serverId = dcId + "_" + partition;
                    protocolLOGGER.finest("Server ID: " + serverId);
                    if (sendToServer(serverId, cm) == NetworkStatus.FAILURE) {
                        protocolLOGGER.severe("Failed to send to server " + serverId);
                        continue;
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                ClientReply cr = readFromServer(serverId);
                if (cr != null && cr.getStatus()) {
                    protocolLOGGER.finest("Got ROT reply " + rm.getKey());
                    rotReplies.add(cr.getRotReply());
                } else {
                    protocolLOGGER.severe("Server could not get the ROT key = " + rm.getKey());
                    rotReplies.add(RotReply.newBuilder().setKey(rm.getKey()).setValue(ByteString.EMPTY).build());
                }
            }
        }
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
            frameworkLOGGER.finer(MessageFormat.format("Sent to server with id= {0} \n{1}", serverId, cm.toString()));
            return NetworkStatus.SUCCESS;
        } catch (Exception e) {
            frameworkLOGGER.warning(Utils.exceptionLogMessge(MessageFormat.format("Problem in sending to server with Id= {0}", serverId), e));
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
            frameworkLOGGER.warning("serversIn= " + serverId + " serversIn.get(serverId) = " + serversIn.get(serverId) + " serversOut.get(serverId)= " + serversOut.get(serverId));
            frameworkLOGGER.warning(Utils.exceptionLogMessge(MessageFormat.format("Problem in reading response from server with Id= {0}", serverId), e));
            return null;
        }
    }
}
