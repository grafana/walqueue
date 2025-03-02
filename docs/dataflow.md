# Data Flow

This diagram illustrates the pull-based data flow implemented in the walqueue system.

```mermaid
sequenceDiagram
    participant FQ as FileQueue
    participant Queue as PromQueue
    participant NM as NetworkManager
    participant WB as WriteBuffer
    participant Remote as Remote Server

    Note over FQ,Remote: Pull-based data flow with RequestMoreSignals

    WB->>NM: Send RequestMoreSignals
    NM->>Queue: Forward RequestMoreSignals
    Queue->>FQ: Request data (networkRequestMoreSignals)
    FQ-->>Queue: Return batch of data (pending items)
    Queue-->>NM: Forward data to appropriate buffer
    NM-->>WB: Send requested data to write buffer
    
    Note over WB: Accumulate until batch size or timeout
    
    WB->>Remote: Send HTTP request with batched data
    Remote-->>WB: Response
```

## Flow Description

1. WriteBuffer components initiate the data flow by sending RequestMoreSignals to the NetworkManager
2. NetworkManager forwards these requests to the Queue component
3. Queue requests data from the FileQueue with networkRequestMoreSignals channel
4. FileQueue returns a batch of pending items
5. Data flows back through the system to the appropriate WriteBuffer
6. WriteBuffer accumulates data until batch size or timeout is reached
7. WriteBuffer sends HTTP request to the remote server