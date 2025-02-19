# **Kafka-Like Server**  

A lightweight, minimal **Kafka-like message broker** built using Java. This project implements basic request parsing and core Kafka APIs (**Produce, Fetch, Metadata**) with in-memory log storage.  

---

## **Features**  
**Kafka Protocol Parsing** – Reads and processes Kafka-like requests with correct formatting.  
**Produce API** – Allows clients to send messages to in-memory topics.  
**Fetch API** – Retrieves stored messages for a given topic.  
**Metadata API** – Lists available topics in the system.  
**Multi-threaded Request Handling** – Uses a thread pool to handle multiple clients simultaneously.  

---

## **Key Java Concepts Used**  

### **1. Networking**  
- `ServerSocket` – Listens for client connections.  
- `Socket` – Handles individual client requests.  
- `InputStream` / `OutputStream` – Reads and writes raw byte data.  

### **2. Concurrency**  
- `ExecutorService` – Manages a thread pool for handling multiple clients.  
- `ConcurrentHashMap` – Stores topic messages in a thread-safe way.  

### **3. Byte Processing**  
- `ByteArrayOutputStream` – Constructs binary responses efficiently.  
- `ByteBuffer` – Converts between bytes and primitive data types.  

---
