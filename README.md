# walqueue

The `walqueue` 

# Queue based remote write client

# Caveat: Consider this the most experimental possible

## Overview

1. The `implementations.prometheus.queue` handles tying all the pieces together into a prometheus compatible fashion.
2. The `serialization` converts an array of series into a serializable format.
3. The `filequeue` is where the buffers are written to. This has a series of files that are committed to disk and then are read.
4. The `network` handles sending data. The data is sharded by the label hash across any number of loops that send data.

Flow

appender -> serialization -> filequeue -> endpoint -> network

## Design Goals

* Consistent memory usage
* Reasonable disk reads/writes
* Replayability
* Versionable

### Consistent memory usage

The system is designed to have a consistent memory usage. This means that the memory should not fluctuate based on the number of metrics written to disk and should be consistent. Memory should be driven by the number of series to be written to disk and by the number of outgoing series.

### Reasonable disk reads/writes

The system should have reasonable disk reads/writes. This means that the disk should not get overloaded by the number of metrics to be written to disk. 

### Replayability

The walqueue should replay metrics in the event of network downtime, or restart. Series TTL will be checked on writing to the `filequeue` and on sending to `network`.

### Versionable

The underlying storage format can and will change. New releases will support previous storage formats for reading.

## Major Parts

### actor like

Underlying each of these major parts is a work loop in the form of the func `run`, each part is single threaded and only exposes a handful of functions for sending and receiving data. Telemetry, configuration and other types of data are passed in via the work loop and handled one at a time. There are some allowances for setting atomic variables for specific scenarios.

This means that the parts are inherently context free and single threaded which greatly simplifies the design. Communication is handled via [mailboxes] that are backed by channels underneath. By default these are asynchronous calls to an unbounded queue. Where that differs will be noted. 

Using mailboxes and messages creates a system that responds to actions instead of polling or calling functions from other threads. This allows us to handle bounded queues easily for if the network is slow or down the `network` queue is bounded and will block on anyone trying to send more work.

In general each actor exposes one to many `Send` function(s), `Start` and `Stop`. 

### serialization

The `serialization` system provides a `prometheus.Appender` interface that is the entrance into the combined system. Each append function encodes the data into a versionable format.

### filequeue

The `filequeue` handles writing and reading data from the `wal` directory. There exists one `filequeue` for each `endpoint` defined. Each file is represented by an atomicly increasing integer that is used to create a file named `<ID>.committed`. The committed name is simply to differentiate it from other files that may get created in the same directory. 

The `filequeue` accepts data `[]byte` and metadata `map[string]string`. These are also written using `msgp` for convenience. The `filequeue` keeps an internal array of files in order by id and fill feed them one by one to the `endpoint`, On startup the `filequeue` will load any existing files into the internal array and start feeding them to `endpoint`. When passing a handle to `endpoint` it passes a callback that actually returns the data and metadata. Once the callback is called then the file is deleted. It should be noted that this is done without touching any state within `filequeue`, keeping the zero mutex promise. It is assumed when the callback is called the data is being processed.

This does mean that the system is not ACID compliant. If a restart happens before memory is written or while it is in the sending queue it will be lost. This is done for performance and simplicity reasons.

### endpoint

The `endpoint` handles uncompressing the data and feeding it to the `network` section. The `endpoint` is the parent of all the other parts and represents a single endpoint to write to. It ultimately controls the lifecycle of each child. 

### network

The `network` consists of two major sections, `manager`,`write_buffer` and `write`. Inspired by the prometheus remote write the signals are placed in a queue by the label hash. This ensures that an out of order sample does not occur within a single instance and provides parallelism. The `manager` handles picking which `write_buffer` to send the data to. Each `write_buffer` then can trigger a `write` request.

The `write_buffer` is responsible for converting a set of `Datum` structs to bytes and sending the data to `write`. The `write/write_buffer` also provides stats, it should be noted these stats are not prometheus or opentelemetry, they are a callback for when stats are updated. This allows the caller to determine how to present the stats. The only requirement is that the callback be threadsafe to the caller.  

### component

At the top level there is a standard component that is responsible for spinning up `endpoints` and passing configuration down.

## Implementation Goals

In normal operation memory should be limited to the scrape, memory waiting to be written to the file queue and memory in the queue to write to the network. This means that memory should not fluctuate based on the number of metrics written to disk and should be consistent.

Replayability, series will be replayed in the event of network downtime, or Alloy restart. Series TTL will be checked on writing to the `filequeue` and on sending to `network`.

### Consistency

Given a certain set of scrapes, the memory usage should be fairly consistent. Once written to disk no reference needs to be made to series. Only incoming and outgoing series contribute to memory. This does mean extreme care is taken to reduce allocations and by extension reduce garbage collection.

### Tradeoffs

In any given system there are tradeoffs, this system goal is to have a consistent memory footprint, reasonable disk reads/writes, and allow replayability. That comes with increased CPU cost, this can range anywhere from 25% to 50% more CPU. 

## Versions

### V1

Version V1 was originally shipped and offered great memory performance at half the memory of the prometheus wal though used 70% more cpu than the wal. Due to a bug where it encoded fields with string identifiers it used more disk space than was neceassary.

Per 10,000 series with 10 labels each used ~400KB of space.

### V2

Version V2 is the newest format and binary encodes items. It stores the raw `prombpb` records on disk alongside additional metadata necessary for logic flow such as Type, IsHistogram, Hash and Timestamp. This is now the default format thought V1 can still be read and sent.

Per 10,000 series with 10 labels each used ~200KB of space and is twice as performant CPU.
