# Tips for debugging the runtime

In this document you'll find a list of tips to debug the runtime.

Suggested tools include:

* [`grpcurl`](https://github.com/fullstorydev/grpcurl) to manually send requests to the runtime
* `docker`/`podman` to run function containers
* A Rust debugger (e.g. Clion)
* Wireshark for analyzing the network

## Test scenario

At the moment e2e tests from the https://github.com/restatedev/e2e repo do not support plugging in the debugger and Wireshark, 
so you manually need to set up and tear down your test scenario.

To do that, you need to:

* Manually build the function images locally (see https://github.com/restatedev/e2e#run-tests)
* Start the function containers mapping the ports to the host ports (e.g. with `podman run -p 9090:8080 restatedev/e2e-counter`)
* Set up the `restate.yml` file in the root of this repo:
  * Make sure the `function_endpoint` is correct
  * Modify the number of peers depending on your test scenario
* Start the peers with `RESTATE_GRPC_PORT=8090;RUST_LOG=debug cargo run --package runtime --bin runtime -- --id <PEER_ID> --configuration-file restate.yml`

Now everything should be in place, and you can send the requests to the runtime by using `grpcurl`, for example:

```bash
grpcurl -plaintext -vv -proto protos/greeter.proto -d '{"name": "John Doe"}' 127.0.0.1:8090 greeter.Greeter/greet
```

Tip: Use `-plaintext` to make sure you can debug the network messages with Wireshark and `-vv` to log all the request/response headers/trailers.

## Setting up Wireshark

1. Make sure you have Wireshark set up for sniffing packets on localhost. On Linux, this usually requires root privileges.
2. Start sniffing on `loopback`
3. Set up the protobuf deserialization by providing the proto contract paths: https://grpc.io/blog/wireshark/#setting-protobuf-search-paths
4. Set up the ports to read as HTTP/2 by going to **Analyze > Decode as**: https://grpc.io/blog/wireshark/#setting-port-traffic-type
5. Set up the filter `http2` in the main top bar.

Now you should be able to see all the HTTP/2 messages, which on top should have the deserialized GRPC messages.
