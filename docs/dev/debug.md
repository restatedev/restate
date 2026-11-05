# Tips for debugging the runtime

In this document you'll find a list of tips to debug the runtime.

Suggested tools include:

* [`grpcurl`](https://github.com/fullstorydev/grpcurl) to manually send requests to the runtime
* `docker`/`podman` to run function containers
* A Rust debugger (e.g. Clion)
* Wireshark for analyzing the network

## Debug info

Dev and test builds only emit line tables: backtraces and profiles show files and lines,
but a debugger can't show variables or types. To debug with full debug info for the
workspace crates, build with:

```shell
CARGO_PROFILE_DEV_DEBUG=full cargo build
```

In an IDE, set the same environment variable in the run or debug configuration. The first
build after switching recompiles the workspace crates. The `release-lite` profile also uses
line tables; use `CARGO_PROFILE_RELEASE_LITE_DEBUG=full` there.

## Setting up Wireshark

1. Make sure you have Wireshark set up for sniffing packets on localhost. On Linux, this usually requires root privileges.
2. Start sniffing on `loopback`
3. Set up the protobuf deserialization by providing the proto contract paths: https://grpc.io/blog/wireshark/#setting-protobuf-search-paths
4. Set up the ports to read as HTTP/2 by going to **Analyze > Decode as**: https://grpc.io/blog/wireshark/#setting-port-traffic-type
5. Set up the filter `http2` in the main top bar.

Now you should be able to see all the HTTP/2 messages, which on top should have the deserialized GRPC messages.
