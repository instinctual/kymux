This repository contains 3 sub-projects:
 - [kynet]: network transport API for QUIC and WebTransport
 - [kyproto]: media transport API to transmit video, audio and input packets
   between two peers (using custom protocol over kynet)
 - [kycom]: IPC component to expose kyproto endpoints over a local IPC (TCP)
   connection

[kynet]: kynet/README.md
[kyproto]: kyproto/README.md
[kycom]: kycom/README.md


```
        Desktop      Browser
       (non-WASM)    (WASM)
           ||          ||
           \/          ||
        +-------+      ||
   IPC  | kycom |      ||
        +-------+      ||
            |          ||
            v          \/
          +---------------+
          |    kyproto    |  media transport API
          +---------------+
                  |
                  v
          +---------------+
          |     kynet     |  network transport API
          +---------------+
```
