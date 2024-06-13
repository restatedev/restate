#!/bin/bash

cat > /dev/stdout << EOF
admin:
  address:
    socket_address: { address: 127.0.0.1, port_value: 9901 }

static_resources:
  listeners:
    - name: lb
      address:
        socket_address:
          address: "127.0.0.1"
          port_value: 19080
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: egress_http
                stream_idle_timeout: 0s
                route_config:
                  name: local_route
                  virtual_hosts:
                    - name: http
                      domains: ["*"]
                      routes:
                        - match: { prefix: / }
                          route:
                            cluster: service
                            timeout: 0s
                http_filters:
                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
  clusters:
    - name: service
      connect_timeout: 1s
      type: STATIC
      lb_policy: ROUND_ROBIN
      http2_protocol_options:
        max_outbound_control_frames: 1000
        connection_keepalive:
          interval: 600s
          timeout: 20s
      load_assignment:
        cluster_name: service
        endpoints:
          - lb_endpoints:
EOF

for port in "$@"
do
    cat > /dev/stdout << EOF
              - endpoint:
                  address:
                    socket_address:
                      address: 127.0.0.1
                      port_value: ${port}
EOF
done
