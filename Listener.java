package com.example;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Listener extends Thread {
    private final ProcessNode node;
    private final Map<Integer, Long> heartbeatMap;
    private volatile boolean running = true;
    private final KafkaConsumer<String, String> consumer;
    private Map<Integer, Double> deputy2Candidates = new ConcurrentHashMap<>();

    public Listener(ProcessNode node, Properties kafkaProps) {
        this.node = node;
        this.heartbeatMap = new ConcurrentHashMap<>();

        Properties props = new Properties();
        props.putAll(kafkaProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "listener-" + node.getPid());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("heartbeat-topic", "dead-topic", "rolechange-topic",
                "election-topic", "promotion-topic", "election-deputy-topic"));
    }

    public Map<Integer, Long> getHeartbeatMap() {
        return heartbeatMap;
    }

    @Override
    public void run() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    String topic = record.topic();
                    String value = record.value();

                    switch (topic) {
                        case "heartbeat-topic":
                            handleHeartbeat(value);
                            break;
                        case "dead-topic":
                            handleDeath(value);
                            break;
                        case "rolechange-topic":
                            handleRoleChange(value);
                            break;
                        case "election-topic":
                            handleElection(value);
                            break;
                        case "promotion-topic":
                            handlePromotion(value);
                            break;
                        case "election-deputy-topic":
                            handleDeputyElection(value);
                            break;
                    }
                }

                // debug output - เฉพาะ Boss เท่านั้น
                if (node.getRole().equals("Boss")) {
                    System.out.println(
                            "PID: " + node.getPid() + "(" + node.getRole() + ") AliveList=" + node.getAliveList());
                    System.out.println(
                            "PID: " + node.getPid() + "(" + node.getRole() + ") BossList=" + node.getBossList());
                }

                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            running = false;
        } finally {
            consumer.close();
        }
    }

    private void handlePromotion(String msg) {
        if (msg.equals("PROMOTE:Deputy2_TO_Deputy1") && node.getRole().equals("Deputy2")) {
            node.setRole("Deputy1");
            // เฉพาะตัวที่เป็น Boss หลังจากได้เป็น Deputy1 จึงจะประกาศ
            if (node.getRole().equals("Boss")) {
                System.out.println("Process " + node.getPid() + " promoted from Deputy2 to Deputy1");
            }
        } else if (msg.startsWith("NEW_DEPUTY2:")) {
            // format: NEW_DEPUTY2:pid
            String[] parts = msg.split(":");
            int newDeputy2Pid = Integer.parseInt(parts[1]);

            // ทุกตัวอัปเดต roleMap และ bossList
            node.getRoleMap().put(newDeputy2Pid, "Deputy2");

            // อัปเดต bossList - ตรวจสอบไม่ให้ซ้ำ
            boolean alreadyExists = false;
            for (String entry : node.getBossList()) {
                if (entry.startsWith(newDeputy2Pid + ":Deputy2:")) {
                    alreadyExists = true;
                    break;
                }
            }

            if (!alreadyExists) {
                node.getBossList().add(newDeputy2Pid + ":Deputy2:alive");
            }

            if (node.getPid() == newDeputy2Pid) {
                node.setRole("Deputy2");
            }

            // เฉพาะ Boss เท่านั้นที่ประกาศ
            if (node.getRole().equals("Boss")) {
                System.out.println("PID: " + node.getPid() + "(" + node.getRole() + ") said Process " + node.getPid()
                        + " acknowledged new Deputy2: PID " + newDeputy2Pid);
            }
        }
    }

    private void handleDeputyElection(String msg) {
        if (msg.equals("ELECT_DEPUTY2:REQUEST") && node.getRole().equals("Follower")) {
            // ส่งคะแนนของตัวเองเพื่อแข่งขันเป็น Deputy2
            double uptimeScore = (System.currentTimeMillis() - node.getStartTime()) / 1000.0;
            double loadScore = new Random().nextDouble() * 10;
            double score = uptimeScore * 0.6 + (10.0 - loadScore) * 0.4;

            String response = "DEPUTY2_CANDIDATE:" + node.getPid() + ":" + score;
            node.getProducer().send(new ProducerRecord<>("election-deputy-topic", response));

            // เฉพาะ Boss เท่านั้นที่ประกาศการส่ง candidate score
            if (node.getRole().equals("Boss")) {
                System.out.println("Process " + node.getPid() + " sent Deputy2 candidate score: " + score);
            }
        } else if (msg.startsWith("DEPUTY2_CANDIDATE:")) {
            handleDeputy2Candidates(msg);
        }
    }

    private void handleDeputy2Candidates(String msg) {
        // format: DEPUTY2_CANDIDATE:pid:score
        String[] parts = msg.split(":");
        int candidatePid = Integer.parseInt(parts[1]);
        double score = Double.parseDouble(parts[2]);

        // ทุกตัวเก็บข้อมูล candidates
        deputy2Candidates.put(candidatePid, score);

        // รอให้ได้คะแนนจาก Follower ทั้งหมด
        long followerCount = node.getAliveList().stream()
                .filter(pid -> {
                    String role = node.getRoleMap().getOrDefault(pid, "Follower");
                    return role.equals("Follower");
                })
                .count();

        // เฉพาะ Boss เท่านั้นที่ประกาศสถานะและตัดสินผล
        if (node.getRole().equals("Boss")) {
            System.out.println("PID: " + node.getPid() + "(" + node.getRole() + ") said Process " + node.getPid()
                    + " Deputy2 election status: " +
                    deputy2Candidates.size() + "/" + followerCount + " candidates");

            if (deputy2Candidates.size() >= followerCount && followerCount > 0) {
                // หา Follower ที่มีคะแนนสูงสุด
                Optional<Map.Entry<Integer, Double>> winner = deputy2Candidates.entrySet().stream()
                        .max((a, b) -> {
                            int scoreCompare = Double.compare(a.getValue(), b.getValue());
                            if (scoreCompare == 0) {
                                return Integer.compare(a.getKey(), b.getKey());
                            }
                            return scoreCompare;
                        });

                if (winner.isPresent()) {
                    int newDeputy2 = winner.get().getKey();

                    // ประกาศ Deputy2 ใหม่
                    String announcement = "NEW_DEPUTY2:" + newDeputy2;
                    node.getProducer().send(new ProducerRecord<>("promotion-topic", announcement));

                    System.out.println("PID: " + node.getPid() + "(" + node.getRole() + ") said Process "
                            + node.getPid() + " elected new Deputy2: PID " + newDeputy2);
                }

                // ล้างข้อมูลการเลือกตั้ง
                deputy2Candidates.clear();
            }
        } else {
            // ตัวอื่นๆ แค่ล้างข้อมูลเมื่อครบแล้ว ไม่ประกาศอะไร
            if (deputy2Candidates.size() >= followerCount && followerCount > 0) {
                deputy2Candidates.clear();
            }
        }
    }

    private void handleHeartbeat(String msg) {
        // format: HEARTBEAT:pid
        String[] parts = msg.split(":");
        int pid = Integer.parseInt(parts[1]);
        heartbeatMap.put(pid, System.currentTimeMillis());

        if (!node.getAliveList().contains(pid)) {
            node.getAliveList().add(pid);
        }
    }

    private void handleDeath(String msg) {
        String[] parts = msg.split(":");
        int deadPid = Integer.parseInt(parts[1]);
        String deadRole = parts[2];

        // ใช้ deathList เป็นตัวป้องกันการประมวลผลซ้ำ
        boolean isNewDeath = !node.getDeathList().contains(deadPid);

        if (isNewDeath) {
            node.getDeathList().add(deadPid);
            node.getAliveList().remove((Integer) deadPid);

            // เฉพาะ Boss เท่านั้นที่ประกาศ และแค่ครั้งแรกเท่านั้น
            if (node.getRole().equals("Boss")) {
                System.out.println("PID: " + node.getPid() + "(" + node.getRole() +
                        ") said Death received: PID=" + deadPid + " Role=" + deadRole);
            }
        }
        // ถ้าไม่ใช่การตายใหม่ จะไม่ทำอะไรเลย
    }

    private void handleRoleChange(String msg) {
        // format: ROLECHANGE:pid:newRole
        String[] parts = msg.split(":");
        int pid = Integer.parseInt(parts[1]);
        String newRole = parts[2];

        // ทุกตัวอัปเดต role map และ boss list
        node.getRoleMap().put(pid, newRole);

        // อัพเดต bossList ให้ถูกต้องตาม role ที่เปลี่ยนไป
        boolean updated = false;
        for (int i = 0; i < node.getBossList().size(); i++) {
            String entry = node.getBossList().get(i);
            if (entry.startsWith(pid + ":")) {
                node.getBossList().set(i, pid + ":" + newRole + ":alive");
                updated = true;
                break;
            }
        }

        // ถ้าไม่พบใน bossList และเป็น leadership role ให้เพิ่มเข้าไป
        if (!updated && (newRole.equals("Boss") || newRole.equals("Deputy1") || newRole.equals("Deputy2"))) {
            node.getBossList().add(pid + ":" + newRole + ":alive");
        }

        // เฉพาะ Boss เท่านั้นที่ประกาศการเปลี่ยน role
        if (node.getRole().equals("Boss")) {
            System.out.println("PID: " + node.getPid() + "(" + node.getRole() + ") said Role change: PID=" + pid
                    + " -> " + newRole);
        }
    }

    private void handleElection(String msg) {
        String[] parts = msg.split(":");
        int pid = Integer.parseInt(parts[1]);
        double score = Double.parseDouble(parts[2]);

        // ทุกตัวเก็บข้อมูล election scores
        node.getElectionScores().put(pid, score);

        // กำหนดจำนวน process ที่ต้องได้คะแนนครบ
        if (node.getElectionScores().size() >= node.getAliveList().size()) {

            // sort score descending, if equal, use higher PID
            List<Map.Entry<Integer, Double>> sorted = new ArrayList<>(node.getElectionScores().entrySet());
            sorted.sort((a, b) -> {
                int scoreCompare = Double.compare(b.getValue(), a.getValue());
                if (scoreCompare == 0) {
                    return Integer.compare(b.getKey(), a.getKey());
                }
                return scoreCompare;
            });

            // ใช้ Set เพื่อไม่ให้ซ้ำกัน
            Set<Integer> assigned = new HashSet<>();

            int bossPid = sorted.get(0).getKey();
            assigned.add(bossPid);

            int deputy1Pid = 0, deputy2Pid = 0;

            // ลูปหา Deputy1 และ Deputy2
            for (int i = 1; i < sorted.size(); i++) {
                int candidate = sorted.get(i).getKey();
                if (!assigned.contains(candidate)) {
                    if (deputy1Pid == 0) {
                        deputy1Pid = candidate;
                        assigned.add(candidate);
                    } else if (deputy2Pid == 0) {
                        deputy2Pid = candidate;
                        assigned.add(candidate);
                        break;
                    }
                }
            }

            // ทุกตัวอัปเดต BossList
            node.getBossList().clear();
            node.getBossList().add(bossPid + ":Boss:alive");
            if (deputy1Pid != 0)
                node.getBossList().add(deputy1Pid + ":Deputy1:alive");
            if (deputy2Pid != 0)
                node.getBossList().add(deputy2Pid + ":Deputy2:alive");

            // ทุกตัวอัพเดต role ของตัวเอง
            if (node.getPid() == bossPid) {
                node.setRole("Boss");
            } else if (node.getPid() == deputy1Pid) {
                node.setRole("Deputy1");
            } else if (node.getPid() == deputy2Pid) {
                node.setRole("Deputy2");
            } else {
                node.setRole("Follower");
            }

            // เฉพาะ Boss ใหม่เท่านั้นที่ประกาศผลการเลือกตั้ง
            if (node.getRole().equals("Boss")) {
                System.out.println("PID: " + node.getPid() + "(" + node.getRole() + ") said Election complete! Boss="
                        + bossPid + " D1=" + deputy1Pid + " D2=" + deputy2Pid);
            }

            // ล้าง election scores สำหรับการเลือกตั้งครั้งต่อไป
            node.getElectionScores().clear();
        }
    }

    public void shutdown() {
        running = false;
        this.interrupt();
    }
}