package com.example;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class HeartbeatSender extends Thread {
    private final ProcessNode node;
    private volatile boolean running = true;
    private long heartbeatCount = 0;

    public HeartbeatSender(ProcessNode node) {
        this.node = node;
        this.setName("HeartbeatSender-" + node.getPid()); // ตั้งชื่อ thread
    }

    @Override
    public void run() {
        System.out.println("HeartbeatSender started for Process " + node.getPid());

        try {
            while (running) {
                String msg = "HEARTBEAT:" + node.getPid();

                // ส่ง heartbeat พร้อม callback เพื่อจัดการ error
                node.getProducer().send(
                        new ProducerRecord<>("heartbeat-topic", msg),
                        new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                if (exception != null) {
                                    System.err.println("Process " + node.getPid() +
                                            " failed to send heartbeat: " + exception.getMessage());
                                } else {
                                    // Uncomment for detailed debugging
                                    // System.out.println("Process " + node.getPid() +
                                    // " sent heartbeat #" + heartbeatCount);
                                }
                            }
                        });

                heartbeatCount++;

                // Debug output ทุก 30 วินาที (30 heartbeats)
                // if (heartbeatCount % 30 == 0) {
                // System.out.println("Process " + node.getPid() + " (" + node.getRole() +
                // ") heartbeat count: " + heartbeatCount);
                // }

                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            System.out.println("HeartbeatSender for Process " + node.getPid() + " interrupted");
            running = false;
        } catch (Exception e) {
            System.err.println("HeartbeatSender for Process " + node.getPid() +
                    " encountered error: " + e.getMessage());
        } finally {
            System.out.println("HeartbeatSender for Process " + node.getPid() + " stopped");
        }
    }

    public void shutdown() {
        System.out.println("Shutting down HeartbeatSender for Process " + node.getPid());
        running = false;
        this.interrupt();
    }

    // เพิ่ม method สำหรับ monitoring
    public long getHeartbeatCount() {
        return heartbeatCount;
    }

    public boolean isRunning() {
        return running && this.isAlive();
    }
}