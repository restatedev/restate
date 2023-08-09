# Service Protocol Wireshark Dissector

This crate contains a dissector for the Service Protocol, which you can use in Wireshark to debug the Restate Service Protocol.

The dissector is composed by two components:

* A lua module, built in Rust, that exposes the decoding functionality as standalone Lua module
* A lua script, that integrates with Wireshark APIs

## How to setup

1. Install Wireshark.
   1. (Optional, but suggested) Make sure you can run Wireshark capture as non-root user. In most of Linux distros this only requires to add your user to `wireshark` group. Check out https://wiki.wireshark.org/CaptureSetup/CapturePrivileges for more details.
2. Install the lua module.
   1. Find out which Lua version Wireshark is using. Check https://wiki.wireshark.org/Lua#getting-started for more details.
   1. Build the module using:
      * Linux: 
         ```
         cargo build -p service_protocol_wireshark_dissector --release --features [lua-version]
         ``` 
         For example: 
         ```
        cargo build --release --features lua51
        ``` 
      * MacOS: 
         ```
         RUSTFLAGS='--cfg uuid_unstable -C link-arg=-undefined -C link-arg=dynamic_lookup' cargo build -p service_protocol_wireshark_dissector --release --features [lua-version]
         ```
         For example: 
         ```
         RUSTFLAGS='--cfg uuid_unstable -C link-arg=-undefined -C link-arg=dynamic_lookup' cargo build -p service_protocol_wireshark_dissector --release --features lua52 --no-default-features
         ```
   1. Find the path(s) where Wireshark looks for Lua modules. You can easily do it by going in Tools > Lua > Evaluate and evaluating the following expression:
      ```lua
      require('mymodule')
      ```
      This will fail printing the scan path of the modules. On my Fedora machine the path is `/usr/lib64/lua/5.1/`.
   1. Copy the compiled `target/release/libservice_protocol_wireshark_dissector.so` in the chosen path, and rename it as `restate_service_protocol_decoder.so`. For example:
      ```shell
      cp target/release/libservice_protocol_wireshark_dissector.so /usr/lib64/lua/5.1/restate_service_protocol_decoder.so
      ```
   1. You can now check if the module is loaded in Wireshark by evaluating the following expression:
      ```lua
      require('restate_service_protocol_decoder')
      ```
3. Install the lua script.
   1. Find out the path where Wireshark looks for plugin scripts. You can find it in Help > About Wireshark > Folders > Personal Lua Plugins. Additional info: https://www.wireshark.org/docs/wsug_html/#ChPluginFolders
   2. Copy the Lua script `dissector.lua` there
   3. Restart Wireshark, make sure the plugin is loaded in Help > About Wireshark > Folders > Plugins. Also make sure the protocol is enabled in Analyze > Enabled protocols.
4. Profit!