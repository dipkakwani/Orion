package edu.msu.cse.causalSpartan.client;

import com.google.protobuf.ByteString;
import edu.msu.cse.dkvf.config.ConfigReader;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String args[]) {
        System.out.println("Starting Client....");
        ConfigReader cnfReader = new ConfigReader(args[0]);
        CausalSpartanClient client = new CausalSpartanClient(cnfReader);
        System.out.println(client.runAll());
        System.out.println("Client started...");
        client.put("k1", "value1".getBytes());
        client.put("k2", "value2".getBytes());
        try {
            System.out.println(new String(client.get("k1"), "UTF-8"));
            System.out.println(new String(client.get("k2"), "UTF-8"));
            List<String> keys = new ArrayList<>();
            keys.add("k1");
            keys.add("k2");
            Map<String, ByteString> key_values = client.rot(keys);
            for (Map.Entry<String, ByteString> entry : key_values.entrySet()) {
                System.out.println(entry.getKey() + "  " + entry.getValue().toStringUtf8());
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
