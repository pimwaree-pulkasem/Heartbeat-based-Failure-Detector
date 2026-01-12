package com.example;

import java.util.*;

public class FailureDetector extends Thread {
    private ProcessNode node;
    private final Listener listener;
    private volatile boolean running = true;

    public FailureDetector(ProcessNode node, Listener listener) {
        this.node = node;
        this.listener = listener;
    }

    @Override
    public void run() {
        try {
            while (running) {
                long now = System.currentTimeMillis();

                // ตรวจสอบ heartbeat timeout ก่อน
                for (Map.Entry<Integer, Long> entry : new HashMap<>(listener.getHeartbeatMap()).entrySet()) {
                    int pid = entry.getKey();
                    long last = entry.getValue();

                    if (now - last > 20000) {
                        String deadRole = node.getRoleMap().getOrDefault(pid, "Follower");
                        boolean isNewDeath = !node.getDeathList().contains(pid);
                        if (isNewDeath) {
                            System.out.println(
                                    "Process " + node.getPid() + " said Process " + pid + " (" + deadRole + ") died");
                        }

                        node.getAliveList().remove((Integer) pid);
                        if (!node.getDeathList().contains(pid)) {
                            node.getDeathList().add(pid);
                        }
                        listener.getHeartbeatMap().remove(pid);

                        // เรียก promote เพื่อจัดการการเลื่อนตำแหน่ง
                        node.promote(pid, deadRole);
                        node.announceDeath(pid, deadRole);
                    }
                }

                // ตรวจสอบ bossList และย้ายตัวที่ตายแล้วไปที่ deathList
                List<String> toRemove = new ArrayList<>();
                boolean hasDeadLeader = false; // เพิ่มตัวแปรเช็คว่ามี leader ตาย

                for (String bossStr : node.getBossList()) {
                    String[] parts = bossStr.split(":");
                    if (parts.length < 3)
                        continue;

                    int bossPid = Integer.parseInt(parts[0]);
                    String role = parts[1];
                    String status = parts[2];

                    // ถ้า pid อยู่ใน deathList หรือไม่มี heartbeat (ตาย)
                    boolean isDead = node.getDeathList().contains(bossPid) ||
                            !listener.getHeartbeatMap().containsKey(bossPid);

                    if (isDead) {
                        // System.out.println("Process " + node.getPid() + " said Moving dead " + role + " (PID: "
                        //         + bossPid + ") from bossList to deathList");

                        // เพิ่มใน deathList ถ้ายังไม่มี
                        if (!node.getDeathList().contains(bossPid)) {
                            node.getDeathList().add(bossPid);
                        }

                        // ตรวจสอบว่าเป็น leader role หรือไม่
                        if (role.equals("Boss") || role.equals("Deputy1") || role.equals("Deputy2")) {
                            hasDeadLeader = true;
                            // เรียก promote เมื่อ leader ตาย
                            node.promote(bossPid, role);
                        }

                        // เตรียมลบออกจาก bossList
                        toRemove.add(bossStr);
                    }
                }

                // ลบรายการที่ตายแล้วออกจาก bossList
                for (String item : toRemove) {
                    node.getBossList().remove(item);
                    // System.out.println("Removed from bossList: " + item);
                }

                // ตรวจสอบสถานการณ์การเลือกตั้ง
                checkElectionConditions();

                if (node.getAliveList().size() == 1) {
                    System.out.println("Process " + node.getPid() + " saidAlert: Only one process left alive.");
                }

                Thread.sleep(5000);
            }
        } catch (InterruptedException e) {
            running = false;
        }
    }

    private void checkElectionConditions() {
        // นับจำนวน leader ที่ยังมีชีวิต
        int aliveBossCount = 0;
        int aliveDeputy1Count = 0;
        int aliveDeputy2Count = 0;

        for (String bossStr : node.getBossList()) {
            String[] parts = bossStr.split(":");
            if (parts.length >= 3) {
                int bossPid = Integer.parseInt(parts[0]);
                String role = parts[1];
                String status = parts[2];

                // ตรวจสอบว่ายังมีชีวิตอยู่จริง
                boolean isAlive = status.equals("alive") &&
                        !node.getDeathList().contains(bossPid) &&
                        listener.getHeartbeatMap().containsKey(bossPid);

                if (isAlive) {
                    switch (role) {
                        case "Boss":
                            aliveBossCount++;
                            break;
                        case "Deputy1":
                            aliveDeputy1Count++;
                            break;
                        case "Deputy2":
                            aliveDeputy2Count++;
                            break;
                    }
                }
            }
        }

        // Debug information
        // System.out.println("Process " + node.getPid() + " - Leadership status: " +
        // "Boss=" + aliveBossCount + ", Deputy1=" + aliveDeputy1Count + ", Deputy2=" +
        // aliveDeputy2Count);
        // System.out.println("Process " + node.getPid() + " - Current bossList: " +
        // node.getBossList());

        // เงื่อนไขการเลือกตั้ง
        if (aliveBossCount == 0 && aliveDeputy1Count == 0 && aliveDeputy2Count == 0) {
            // ไม่มี leader เลย -> เลือกตั้งใหม่ทั้งหมด
            System.out.println("Process " + node.getPid() + " said No leaders alive → Process " + node.getPid()
                    + " starting election");
            node.startElection();
        } else if (node.getBossList().isEmpty()) {
            // bossList ว่างเปล่า -> เลือกตั้งใหม่
            System.out.println("Process " + node.getPid() + " said BossList is empty → Process " + node.getPid()
                    + " starting election");
            node.startElection();
        }

        // เงื่อนไขเพิ่มเติม: หาก process นี้เป็น leader แต่ไม่อยู่ใน bossList
        String myRole = node.getRole();
        if ((myRole.equals("Boss") || myRole.equals("Deputy1") || myRole.equals("Deputy2")) &&
                !isInBossList(node.getPid())) {
            // เพิ่มตัวเองเข้า bossList
            node.getBossList().add(node.getPid() + ":" + myRole + ":alive");
            // System.out.println("Added self to bossList: " + node.getPid() + ":" +
            // myRole);
        }
    }

    private boolean isInBossList(int pid) {
        for (String bossStr : node.getBossList()) {
            String[] parts = bossStr.split(":");
            if (parts.length >= 1) {
                int bossPid = Integer.parseInt(parts[0]);
                if (bossPid == pid) {
                    return true;
                }
            }
        }
        return false;
    }

    public void shutdown() {
        running = false;
        this.interrupt();
    }
}