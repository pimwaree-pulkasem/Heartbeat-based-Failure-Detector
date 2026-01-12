# Heartbeat-based-Failure-Detector
## Overview

This project is a **Java-based distributed system simulation** that demonstrates how multiple processes can work together, monitor each other’s health, and detect failures using a **heartbeat mechanism**.

The system is designed to show fundamental concepts used in real-world distributed systems, such as:

- Heartbeat communication
- Failure detection
- Leader (Boss) and backup roles (Deputy / Deputy2)
- Multi-threading
- Fault tolerance

This project is suitable for showcasing understanding of **backend systems, distributed computing concepts, and concurrent programming**.

---

## Project Structure

```
├── Main.java
├── ProcessNode.java
├── Listener.java
├── HeartbeatSender.java
└── FailureDetector.java
```

---

## System Concept

Each running instance represents a **process node** in a distributed system.

- Every process periodically sends a **heartbeat message** to inform others that it is still alive.
- If a process stops sending heartbeats within a specified time window, it is considered **failed**.
- The system maintains different roles:
  - **Boss (Leader)** – main coordinator
  - **Deputy / Deputy2** – backup processes that can take over if the Boss fails

---

## File Descriptions

### 1. `Main.java`

**Entry point of the application**

Responsibilities:
- Starts the program
- Reads initial configuration (e.g., process ID)
- Creates a `ProcessNode` instance
- Initializes and starts background threads:
  - Listener
  - HeartbeatSender
  - FailureDetector

This file acts as the **bootstrapper** of the system.

---

### 2. `ProcessNode.java`

**Core data model representing a process**

Responsibilities:
- Stores process information such as:
  - Process ID (PID)
  - Role (Boss, Deputy, Deputy2)
  - Known nodes in the system
- Tracks the alive/dead status of other processes
- Acts as a shared state accessed by multiple threads

This class functions as the **central state holder** for each node.

---

### 3. `Listener.java`

**Handles incoming network messages**

Responsibilities:
- Listens for incoming socket connections
- Receives heartbeat messages from other processes
- Updates last-seen timestamps for each process
- Processes role-related or control messages

This component ensures the process is aware of the current state of other nodes.

---

### 4. `HeartbeatSender.java`

**Periodically sends heartbeat signals**

Responsibilities:
- Sends heartbeat messages at fixed intervals
- Notifies other processes that this node is still alive
- Runs continuously in its own thread

Heartbeat messages help prevent false failure detection and maintain system stability.

---

### 5. `FailureDetector.java`

**Detects failed processes**

Responsibilities:
- Monitors the time since the last heartbeat from each process
- Determines whether a process has failed based on timeout rules
- Updates process status accordingly
- Triggers role changes if necessary (e.g., Deputy replacing Boss)

This class provides **fault tolerance** to the system.

---

## System Workflow

1. Application starts via `Main.java`
2. A `ProcessNode` is created for the current process
3. Background threads are launched:
   - Listener (receive messages)
   - HeartbeatSender (send heartbeats)
   - FailureDetector (monitor failures)
4. Processes continuously exchange heartbeat messages
5. If a process stops responding:
   - FailureDetector marks it as failed
   - System adjusts roles if needed

---

