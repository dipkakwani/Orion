package edu.msu.cse.causalSpartan.server;

import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import com.google.protobuf.ByteString;
import edu.msu.cse.dkvf.ClientMessageAgent;
import edu.msu.cse.dkvf.DKVFServer;
import edu.msu.cse.dkvf.Storage.StorageStatus;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata;
import edu.msu.cse.dkvf.metadata.Metadata.ClientReply;
import edu.msu.cse.dkvf.metadata.Metadata.DSVMessage;
import edu.msu.cse.dkvf.metadata.Metadata.DcTimeItem;
import edu.msu.cse.dkvf.metadata.Metadata.GetMessage;
import edu.msu.cse.dkvf.metadata.Metadata.GetReply;
import edu.msu.cse.dkvf.metadata.Metadata.PutMessage;
import edu.msu.cse.dkvf.metadata.Metadata.PutReply;
import edu.msu.cse.dkvf.metadata.Metadata.RotMessage;
import edu.msu.cse.dkvf.metadata.Metadata.RotReply;
import edu.msu.cse.dkvf.metadata.Metadata.Record;
import edu.msu.cse.dkvf.metadata.Metadata.SliceRequestMessage;
import edu.msu.cse.dkvf.metadata.Metadata.SliceReplyMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ReplicateMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;

public class CausalSpartanServer extends DKVFServer {

    List<Long> dsv;
    int dcId;// datacenter id
    int pId; // partition id
    int numOfDatacenters;
    int numOfPartitions;

    // GST computation
    ArrayList<AtomicLong> vv;
    HashMap<Integer, List<Long>> childrenVvs;

    // Tree structure
    List<Integer> childrenPIds;
    int parentPId;

    // ROT
    long rotCount;
    final Map<Long, RotReply.Builder> rotTxn = new HashMap<>();

    // intervals
    int heartbeatInterval;
    int dsvComutationInterval;

    // Heartbeat
    long timeOfLastRepOrHeartbeat;

    Object putLock = new Object(); // It is necessary to make sure that
    // replicates are send
    // FIFO

    public CausalSpartanServer(ConfigReader cnfReader) {
        super(cnfReader);
        HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();

        dcId = new Integer(protocolProperties.get("dc_id").get(0));
        pId = new Integer(protocolProperties.get("p_id").get(0));

        parentPId = new Integer(protocolProperties.get("parent_p_id").get(0));
        childrenPIds = new ArrayList<Integer>();
        if (protocolProperties.get("children_p_ids") != null) {
            for (String id : protocolProperties.get("children_p_ids")) {
                childrenPIds.add(new Integer(id));
            }
        }

        numOfDatacenters = new Integer(protocolProperties.get("num_of_datacenters").get(0));
        numOfPartitions = new Integer(protocolProperties.get("num_of_partitions").get(0));

        heartbeatInterval = new Integer(protocolProperties.get("heartbeat_interval").get(0));
        dsvComutationInterval = new Integer(protocolProperties.get("dsv_comutation_interval").get(0));

        vv = new ArrayList<>();
        ArrayList<Long> allZero = new ArrayList<>();
        for (int i = 0; i < numOfDatacenters; i++) {
            vv.add(i, new AtomicLong(0));
            allZero.add(new Long(0));
        }


        childrenVvs = new HashMap<>();
        for (int cpId : childrenPIds) {
            childrenVvs.put(cpId, allZero);
        }

        dsv = new ArrayList<>();
        for (int i = 0; i < numOfDatacenters; i++) {
            dsv.add(i, new Long(0));
        }

        // Scheduling periodic operations
        ScheduledExecutorService heartbeatTimer = Executors.newScheduledThreadPool(1);
        ScheduledExecutorService dsvComputationTimer = Executors.newScheduledThreadPool(1);

        heartbeatTimer.scheduleAtFixedRate(new HeartbeatSender(this), 0, heartbeatInterval, TimeUnit.MILLISECONDS);
        dsvComputationTimer.scheduleAtFixedRate(new DsvComputation(this), 0, dsvComutationInterval, TimeUnit.MILLISECONDS);
    }

    public void handleClientMessage(ClientMessageAgent cma) {
        if (cma.getClientMessage().hasGetMessage()) {
            handleGetMessage(cma);
        } else if (cma.getClientMessage().hasPutMessage()) {
            handlePutMessage(cma);
        } else {
            handleRotMessage(cma);
        }

    }

    private void handleGetMessage(ClientMessageAgent cma) {
        GetMessage gm = cma.getClientMessage().getGetMessage();
        updateDsv(gm.getDsvItemList());
        List<Record> result = new ArrayList<>();
        StorageStatus ss = read(gm.getKey(), isVisible, result);
        ClientReply cr = null;
        if (ss == StorageStatus.SUCCESS) {
            Record rec = result.get(0);
            List<DcTimeItem> newDs = updateDS(rec.getSr(), rec.getUt(), rec.getDsItemList());
            cr = ClientReply.newBuilder().setStatus(true).setGetReply(GetReply.newBuilder().setValue(rec.getValue()).addAllDsItem(newDs).addAllDsvItem(dsv)).build();
        } else {
            cr = ClientReply.newBuilder().setStatus(false).build();
        }
        cma.sendReply(cr);
    }

    private void updateDsv(List<Long> dsvItemList) {
        if (dsvItemList == null || dsvItemList.isEmpty())
            return;
        synchronized (dsv) {
            for (int i = 0; i < dsv.size(); i++) {
                dsv.set(i, Math.max(dsv.get(i), dsvItemList.get(i)));
            }
        }

    }

    Predicate<Record> isVisible = (Record r) -> {
        if (dcId == r.getSr())
            return true;

        for (int i = 0; i < r.getDsItemCount(); i++) {
            DcTimeItem dti = r.getDsItem(i);
            if (dsv.get(dti.getDcId()) < dti.getTime())
                return false;
        }
        return true;

    };

    // For ROT
    static Predicate<Record> isVisibleSnapshot(int dcId, List<Long> sv) {
        return new Predicate<Record>() {
            @Override
            public boolean test(Record r) {
                if (dcId == r.getSr())
                    return true;

                for (int i = 0; i < r.getDsItemCount(); i++) {
                    DcTimeItem dti = r.getDsItem(i);
                    if (sv.get(dti.getDcId()) < dti.getTime())
                        return false;
                }
                return true;
            }
        };
    }

    private void handlePutMessage(ClientMessageAgent cma) {
        PutMessage pm = cma.getClientMessage().getPutMessage();
        long dt = Utils.maxDsTime(pm.getDsItemList());
        updateHlc(dt);
        Record rec = null;
        synchronized (putLock) {
            rec = Record.newBuilder().setValue(pm.getValue()).setUt(vv.get(dcId).get()).setSr(dcId).addAllDsItem(pm.getDsItemList()).build();
            sendReplicateMessages(pm.getKey(), rec); // The order is different than the paper
            // algorithm. We first send replicate to
            // insure a version with smaller
            // timestamp is replicated sooner.
        }
        StorageStatus ss = insert(pm.getKey(), rec);
        ClientReply cr = null;

        if (ss == StorageStatus.SUCCESS) {
            cr = ClientReply.newBuilder().setStatus(true).setPutReply(PutReply.newBuilder().setUt(rec.getUt()).setSr(dcId)).build();
        } else {
            cr = ClientReply.newBuilder().setStatus(false).build();
        }
        cma.sendReply(cr);
    }

    private void handleRotMessage(ClientMessageAgent cma) {
        protocolLOGGER.finest("ROT Message received");
        RotMessage rm = cma.getClientMessage().getRotMessage();
        // Snapshot vector for ROT
        List<Long> sv;

        // Update DSV
        updateDsv(rm.getDsvItemList());
        synchronized (dsv) {
            for (Map.Entry<Integer, Long> dsEntry : rm.getDsItemsMap().entrySet()) {
                dsv.set(dsEntry.getKey(), Math.max(dsv.get(dsEntry.getKey()), dsEntry.getValue()));
            }
            sv = new ArrayList<>(dsv);
        }

        final RotReply.Builder rotBuilder = RotReply.newBuilder().setCount(rm.getKeysCount());

        // Generate ROT ID
        long rotID;
        synchronized (rotTxn) {
            rotID = rotCount++;
            rotTxn.put(rotID, rotBuilder);
        }

        // send requests for reading keys to servers
        for (String key : rm.getKeysList()) {
            protocolLOGGER.finest("Searching key: " + key);
            try {
                int p = findPartition(key);
                // If key is not present in current partition
                if (p != pId) {
                    protocolLOGGER.finest("Sending slice request message");
                    SliceRequestMessage sreq = Metadata.SliceRequestMessage.newBuilder()
                            .setPId(pId).setRotID(rotID)
                            .setKey(key).addAllSv(sv).build();
                    ServerMessage sm = ServerMessage.newBuilder().setSreqMessage(sreq).build();
                    sendToServerViaChannel(dcId + "_" + p, sm);
                } else {
                    protocolLOGGER.finest("Key available locally");
                    List<Record> result = new ArrayList<>();
                    StorageStatus ss = read(key, isVisibleSnapshot(dcId, sv), result);
                    if (ss == StorageStatus.SUCCESS) {
                        Record rec = result.get(0);
                        protocolLOGGER.finest("Found key " + key + " value: " + rec.getValue().toStringUtf8());
                        List<DcTimeItem> newDs = updateDS(rec.getSr(), rec.getUt(), rec.getDsItemList());
                        protocolLOGGER.finest("Acquiring rotBuilder lock " + Thread.currentThread().getName());
                        protocolLOGGER.finest("Lock status " + Thread.holdsLock(rotBuilder));
                        // Take max DS
                        synchronized (rotBuilder) {
                            Map<Integer, Long> ds = rotBuilder.getDsItemsMap();
                            rotBuilder.putAllDsItems(updateDS(ds, newDs));
                            rotBuilder.putAllDsItems(ds);
                            rotBuilder.putKeyValue(key, rec.getValue());
                        }
                        protocolLOGGER.finest("Released rotBuilder lock" + Thread.currentThread().getName());
                    }
                    else {
                       protocolLOGGER.severe("Could not find the key locally " + key + " for ROT " + rotID);
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        protocolLOGGER.finest("Waiting for values to arrive ROT ID " + rotID);
        // Wait for all the values to arrive
        synchronized (rotBuilder) {
            try {
                while (rotBuilder.getKeyValueCount() != rotBuilder.getCount())
                    rotBuilder.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        protocolLOGGER.finest("ROT FINISHED " + rotID);
        for (Map.Entry<String, ByteString> entry : rotBuilder.getKeyValueMap().entrySet()) {
            protocolLOGGER.finest(entry.getKey() + " " + entry.getValue().toStringUtf8());
        }

        // Send the reply to the client
        ClientReply cr = ClientReply.newBuilder().setStatus(true)
                .setRotReply(rotBuilder.addAllDsvItem(dsv)).build();
        cma.sendReply(cr);
    }

    private void sendReplicateMessages(String key, Record recordToReplicate) {
        ServerMessage sm = ServerMessage.newBuilder().setReplicateMessage(ReplicateMessage.newBuilder().setDcId(dcId).setKey(key).setRec(recordToReplicate)).build();
        for (int i = 0; i < numOfDatacenters; i++) {
            if (i == dcId)
                continue;
            String id = i + "_" + pId;

            protocolLOGGER.finer(MessageFormat.format("Sending replicate message to {0}: {1}", id, sm.toString()));
            sendToServerViaChannel(id, sm);
        }
        timeOfLastRepOrHeartbeat = Utils.getPhysicalTime(); //we don't need to synchronize for it, because it is not critical
    }

    private void updateHlc(long dt) {
        long vv_l = Utils.getL(vv.get(dcId).get());
        long physicalTime = Utils.getPhysicalTime();
        long dt_l = Utils.getL(dt);

        long newL = Math.max(Math.max(vv_l, dt_l), Utils.shiftToHighBits(physicalTime));

        long vv_c = Utils.getC(vv.get(dcId).get());
        long dt_c = Utils.getC(dt);
        long newC;
        if (newL == vv_l && newL == dt_l)
            newC = Math.max(vv_c, dt_c) + 1;
        else if (newL == vv_l)
            newC = vv_c + 1;
        else if (newL == dt_l)
            newC = dt_c + 1;
        else
            newC = 0;
        vv.get(dcId).set(newL + newC);
    }

    void updateHlc() {
        long vv_l = Utils.getL(vv.get(dcId).get());
        long physicalTime = Utils.getPhysicalTime();

        long newL = Math.max(vv_l, Utils.shiftToHighBits(physicalTime));

        long vv_c = Utils.getC(vv.get(dcId).get());
        long newC;
        if (newL == vv_l)
            newC = vv_c + 1;
        else
            newC = 0;
        vv.get(dcId).set(newL + newC);
    }

    private int findPartition(String key) throws NoSuchAlgorithmException {
        long hash = edu.msu.cse.dkvf.Utils.getMd5HashLong(key);
        return (int) (hash % numOfPartitions);
    }

    public void handleServerMessage(ServerMessage sm) {
        if (sm.hasReplicateMessage()) {
            handleReplicateMessage(sm);
        } else if (sm.hasHeartbeatMessage()) {
            handleHearbeatMessage(sm);
        } else if (sm.hasVvMessage()) {
            handleVvMessage(sm);
        } else if (sm.hasDsvMessage()) {
            handleDsvMessage(sm);
        } else if (sm.hasSreqMessage()) {
            handleSreqMessage(sm);
        } else if (sm.hasSrepMessage()) {
            handleSrepMessage(sm);
        }
    }

    private void handleReplicateMessage(ServerMessage sm) {
        protocolLOGGER.finer(MessageFormat.format("Received replicate message: {0}", sm.toString()));
        int senderDcId = sm.getReplicateMessage().getDcId();
        Record d = sm.getReplicateMessage().getRec();
        insert(sm.getReplicateMessage().getKey(), d);
        vv.get(senderDcId).set(d.getUt());
    }

    void handleHearbeatMessage(ServerMessage sm) {
        int senderDcId = sm.getHeartbeatMessage().getDcId();
        vv.get(senderDcId).set(sm.getHeartbeatMessage().getTime());
    }

    void handleVvMessage(ServerMessage sm) {
        int senderPId = sm.getVvMessage().getPId();
        List<Long> receivedVv = sm.getVvMessage().getVvItemList();
        protocolLOGGER.finest("Recieved" + sm.toString());
        childrenVvs.put(senderPId, receivedVv);
    }

    void handleDsvMessage(ServerMessage sm) {
        protocolLOGGER.finest(sm.toString());
        setDsv(sm.getDsvMessage().getDsvItemList());
        sm = ServerMessage.newBuilder().setDsvMessage(DSVMessage.newBuilder().addAllDsvItem(dsv)).build();
        sendToAllChildren(sm);
    }

    void handleSreqMessage(ServerMessage sm) {
        SliceRequestMessage sreq = sm.getSreqMessage();
        List<Record> result = new ArrayList<>();
        StorageStatus ss = read(sreq.getKey(), isVisibleSnapshot(dcId, sreq.getSvList()), result);
        if (ss == StorageStatus.SUCCESS) {
            Record rec = result.get(0);
            List<DcTimeItem> newDs = updateDS(rec.getSr(), rec.getUt(), rec.getDsItemList());
            SliceReplyMessage srep = SliceReplyMessage.newBuilder()
                    .setRotID(sreq.getRotID())
                    .setKey(sreq.getKey())
                    .setValue(rec.getValue())
                    .addAllDs(newDs)
                    .build();
            ServerMessage reply = ServerMessage.newBuilder().setSrepMessage(srep).build();
            sendToServerViaChannel(dcId + "_" + sreq.getPId(), reply);
        }
        else {
            protocolLOGGER.severe("Could not find the key "+ sreq.getKey() + " for ROT " + sreq.getRotID());
        }
    }

    void handleSrepMessage(ServerMessage sm) {
        SliceReplyMessage srep = sm.getSrepMessage();
        RotReply.Builder rotBuilder = rotTxn.get(srep.getRotID());
        // Take max DS
        synchronized (rotBuilder) {
            Map<Integer, Long> ds = rotBuilder.getDsItemsMap();
            updateDS(ds, srep.getDsList());
            rotBuilder.putAllDsItems(ds);
            rotBuilder.putKeyValue(srep.getKey(), srep.getValue());
            // If the current key is the last key, notify the waiting threads
            if (rotBuilder.getKeyValueCount() == rotBuilder.getCount()) {
                rotBuilder.notifyAll();
            }
        }
    }

    void sendToAllChildren(ServerMessage sm) {
        for (Map.Entry<Integer, List<Long>> child : childrenVvs.entrySet()) {
            int childId = child.getKey();
            sendToServerViaChannel(dcId + "_" + childId, sm);
        }
    }

    void setDsv(List<Long> newDsv) {
        synchronized (dsv) {
            for (int i = 0; i < newDsv.size(); i++)
                dsv.set(i, newDsv.get(i));
        }
    }

    private List<DcTimeItem> updateDS(int dc, long time, List<DcTimeItem> ds) {
        List<DcTimeItem> result = new ArrayList<>();

        for (int i = 0; i < ds.size(); i++) {
            if (ds.get(i).getDcId() != dc)
                result.add(ds.get(i));
            else {
                long oldTime = ds.get(i).getTime();
                result.add(DcTimeItem.newBuilder().setDcId(dc).setTime(Math.max(time, oldTime)).build());
            }
        }
        return result;
    }

    // Updates DS by taking max of two DS
    private Map<Integer, Long> updateDS(Map<Integer, Long> ds1, List<DcTimeItem> ds2) {
        System.out.println(ds2.size());
        Map<Integer, Long> result = new HashMap<>(ds1);
        for (DcTimeItem ds_item : ds2) {
            System.out.println(ds_item.getDcId() + " " + ds_item.getTime());
            if (!ds1.containsKey(ds_item.getDcId())
                    || ds1.get(ds_item.getDcId()) < ds_item.getTime()) {
                System.out.println("Adding " + ds_item.getDcId());
                result.put(ds_item.getDcId(), ds_item.getTime());
                System.out.println("Added " + ds_item.getDcId());
            }
        }
        System.out.println("Returning update DS");
        return result;
    }
}
