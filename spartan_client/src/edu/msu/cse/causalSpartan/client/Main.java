package edu.msu.cse.causalSpartan.client;

import edu.msu.cse.dkvf.config.ConfigReader;

import java.io.UnsupportedEncodingException;

public class Main {
    public static void main(String args[]) {
        System.out.println("Starting Client....");
        ConfigReader cnfReader = new ConfigReader(args[0]);
        CausalSpartanClient client = new CausalSpartanClient(cnfReader);
        System.out.println(client.runAll());
        System.out.println("Client started...");
        client.put("k1", "value1".getBytes());
        try {
            System.out.println(new String(client.get("k1"), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
