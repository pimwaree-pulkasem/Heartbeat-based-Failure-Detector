package com.example;

import java.util.*;

public class Main {
    public static void main(String[] args) {
        // Kafka properties
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Set<Integer> usedPIDs = new HashSet<>();
        Random rnd = new Random();

        List<ProcessNode> nodes = new ArrayList<>();
        List<HeartbeatSender> heartbeats = new ArrayList<>();
        List<Listener> listeners = new ArrayList<>();
        List<FailureDetector> detectors = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            int pid;
            do {
                pid = 100 + rnd.nextInt(900);
            } while (usedPIDs.contains(pid));
            usedPIDs.add(pid);

            ProcessNode node = new ProcessNode(pid, kafkaProps);
            nodes.add(node);

            HeartbeatSender hb = new HeartbeatSender(node);
            heartbeats.add(hb);

            Listener listener = new Listener(node, kafkaProps);
            listeners.add(listener);

            FailureDetector fd = new FailureDetector(node, listener);
            detectors.add(fd);
        }

        // start all threads
        for (Listener l : listeners)
            l.start();
        for (HeartbeatSender hb : heartbeats)
            hb.start();
        for (FailureDetector fd : detectors)
            fd.start();

        System.out.println("All processes started. Running system...");

        try {
            // รอให้ election รอบแรกเสร็จ
            Thread.sleep(20000);

            // ค้นหา Boss, Deputy1
            List<Integer> leaderIndexes = new ArrayList<>();
            for (int i = 0; i < nodes.size(); i++) {
                String role = nodes.get(i).getRole();
                if ("Boss".equals(role)) {
                    leaderIndexes.add(i);
                }
            }

            System.out.println("\n=== Simulate Boss death ===");
            for (int idx : leaderIndexes) {
                ProcessNode leader = nodes.get(idx);
                System.out.println("Killing " + leader.getRole() + " PID = " + leader.getPid());

                heartbeats.get(idx).shutdown();
                listeners.get(idx).shutdown();
                detectors.get(idx).shutdown();
            }

            // ให้ระบบ detect และเลือกตั้งใหม่
            Thread.sleep(40000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // shutdown threads ที่เหลือ
        System.out.println("\n=== Shutdown all threads ===");
        for (HeartbeatSender hb : heartbeats)
            hb.shutdown();
        for (Listener l : listeners)
            l.shutdown();
        for (FailureDetector fd : detectors)
            fd.shutdown();
        for (ProcessNode node : nodes)
            node.closeProducer();

        System.out.println("All threads stopped. System shutdown complete.");
    }
}