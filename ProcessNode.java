package com.example;

import org.apache.kafka.clients.producer.*;
import java.util.*;
import java.util.concurrent.*;

public class ProcessNode {
    private int pid;
    private String role = null;
    CopyOnWriteArrayList<Integer> aliveList = new CopyOnWriteArrayList<>();
    CopyOnWriteArrayList<String> bossList = new CopyOnWriteArrayList<>();
    private List<Integer> deathList = new CopyOnWriteArrayList<>();
    private Map<Integer, Double> electionScores = new ConcurrentHashMap<>();
    private Map<Integer, String> roleMap = new ConcurrentHashMap<>();
    private long startTime;

    // Kafka producer shared
    private final KafkaProducer<String, String> producer;

    public ProcessNode(int pid, Properties kafkaProps) {
        this.pid = pid;
        this.startTime = System.currentTimeMillis();
        this.role = "Follower";
        this.producer = new KafkaProducer<>(kafkaProps);

        System.out.println("New process PID = " + pid + " added as Follower");
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

    // getters
    public int getPid() {
        return pid;
    }

    public String getRole() {
        return role;
    }

    public List<Integer> getAliveList() {
        return aliveList;
    }

    public List<Integer> getDeathList() {
        return deathList;
    }

    public Map<Integer, Double> getElectionScores() {
        return electionScores;
    }

    public List<String> getBossList() {
        return bossList;
    }

    public long getStartTime() {
        return startTime;
    }

    public Map<Integer, String> getRoleMap() {
        return roleMap;
    }

    // setters
    public synchronized void setRole(String newRole) {
        this.role = newRole;
        roleMap.put(pid, newRole);
        announceRoleChange(newRole);
        // System.out.println("Process " + pid + " role set to " + newRole);
    }

    // Promotion
    public synchronized void promote(int deadPid, String deadRole) {
        // System.out
        // .println("Process " + pid + " received promotion signal - Dead: PID " +
        // deadPid + " Role: " + deadRole);

        if (deadRole.equals("Deputy2")) {
            if (role.equals("Follower")) {
                // หา Follower ที่มีคะแนนสูงสุดมาเป็น Deputy2
                electNewDeputy2();
            }
        } else if (deadRole.equals("Deputy1")) {
            if (role.equals("Deputy2")) {
                setRole("Deputy1");
                // หา Follower ใหม่มาเป็น Deputy2
                electNewDeputy2();
            } else if (role.equals("Follower")) {
                // หา Follower ที่มีคะแนนสูงสุดมาเป็น Deputy2
                electNewDeputy2();
            }
        } else if (deadRole.equals("Boss")) {
            if (role.equals("Deputy1")) {
                setRole("Boss");
                // ให้ Deputy2 เลื่อนขึ้นเป็น Deputy1
                promoteDeputy2ToDeputy1();
                // หา Follower ใหม่มาเป็น Deputy2
                electNewDeputy2();
            } else if (role.equals("Deputy2")) {
                // รอให้ Deputy1 เลื่อนขึ้นเป็น Boss ก่อน
                // แล้วตัวเองจะเลื่อนขึ้นเป็น Deputy1
            } else if (role.equals("Follower")) {
                // รอให้ hierarchy เลื่อนขึ้นก่อน
            }
        }

        // อัปเดต bossList
        updateBossListAfterDeath(deadPid, deadRole);
    }

    private void promoteDeputy2ToDeputy1() {
        // ส่งสัญญาณให้ Deputy2 เลื่อนขึ้นเป็น Deputy1
        String msg = "PROMOTE:Deputy2_TO_Deputy1";
        producer.send(new ProducerRecord<>("promotion-topic", msg));
    }

    private void electNewDeputy2() {
        // เริ่มการเลือกตั้ง Deputy2 ใหม่
        String msg = "ELECT_DEPUTY2:REQUEST";
        producer.send(new ProducerRecord<>("election-deputy-topic", msg));
    }

    private void updateBossListAfterDeath(int deadPid, String deadRole) {
        // อัปเดต bossList หลังจากมีคนตาย
        for (int i = 0; i < bossList.size(); i++) {
            String entry = bossList.get(i);
            String[] parts = entry.split(":");
            if (parts.length >= 3) {
                int entryPid = Integer.parseInt(parts[0]);
                String entryRole = parts[1];

                if (entryPid == deadPid && entryRole.equals(deadRole)) {
                    // เปลี่ยนสถานะเป็น dead
                    bossList.set(i, deadPid + ":" + deadRole + ":dead");
                    break;
                }
            }
        }
    }

    // Election
    public void startElection() {
        double uptimeScore = (System.currentTimeMillis() - startTime) / 1000.0;
        double loadScore = new Random().nextDouble() * 10;
        double score = uptimeScore * 0.6 + (10.0 - loadScore) * 0.4;

        electionScores.put(pid, score);

        String msg = "ELECTION:" + pid + ":" + score;
        producer.send(new ProducerRecord<>("election-topic", msg));
        System.out.println("Process " + pid + " sent score " + score + " via Kafka");
    }

    public void announceDeath(int deadPid, String deadRole) {
        // mark dead in bossList
        String newRole = deadRole;
        for (int i = 0; i < bossList.size(); i++) {
            String[] parts = bossList.get(i).split(":");
            int pidInList = Integer.parseInt(parts[0]);
            String roleInList = parts[1];
            if (pidInList == deadPid) {
                if (roleInList.equals("Boss")) {
                    newRole = "Ex-Boss";
                    bossList.set(i, pidInList + ":Ex-Boss:dead");
                } else if (roleInList.equals("Deputy1")) {
                    newRole = "Ex-Deputy1";
                    bossList.set(i, pidInList + ":Ex-Deputy1:dead");
                } else if (roleInList.equals("Deputy2")) {
                    newRole = "Ex-Deputy2";
                    bossList.set(i, pidInList + ":Ex-Deputy2:dead");
                }
            }
        }
        String msg = "DEAD:" + deadPid + ":" + newRole;
        producer.send(new ProducerRecord<>("dead-topic", msg));

    }

    public void announceRoleChange(String newRole) {
        String msg = "ROLECHANGE:" + pid + ":" + newRole;
        producer.send(new ProducerRecord<>("rolechange-topic", msg));
        // System.out.println("Kafka send -> " + msg);
    }

    public void closeProducer() {
        producer.close();
    }
}