# Tips for debugging the runtime

In this document you'll find a list of tips to debug the runtime.

Suggested tools include:

* [`grpcurl`](https://github.com/fullstorydev/grpcurl) to manually send requests to the runtime
* `docker`/`podman` to run function containers
* A Rust debugger (e.g. Clion)
* Wireshark for analyzing the network

## Setting up Wireshark

1. Make sure you have Wireshark set up for sniffing packets on localhost. On Linux, this usually requires root privileges.
2. Start sniffing on `loopback`
3. Set up the protobuf deserialization by providing the proto contract paths: https://grpc.io/blog/wireshark/#setting-protobuf-search-paths
4. Set up the ports to read as HTTP/2 by going to **Analyze > Decode as**: https://grpc.io/blog/wireshark/#setting-port-traffic-type
5. Set up the filter `http2` in the main top bar.

Now you should be able to see all the HTTP/2 messages, which on top should have the deserialized GRPC messages.
