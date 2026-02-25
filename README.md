[![PyPI version](https://img.shields.io/pypi/v/paravon.svg)](https://pypi.org/project/paravon)
[![Python versions](https://img.shields.io/pypi/pyversions/paravon.svg)](https://pypi.org/project/paravon)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/mistersouls/paravon/blob/main/LICENSE)
[![Build Status](https://github.com/mistersouls/paravon/actions/workflows/ci.yaml/badge.svg)](https://github.com/mistersouls/paravon/actions/workflows/ci.yaml?query=branch%3Amain)


# Paravon

**Paravon** is a leaderless, peer‑to‑peer distributed key-value database designed to provide
predictable data evolution, deterministic convergence, and secure communication across all nodes.
It relies on a consistent‑hashing ring for partitioning and replication, and maintains a clear,
deterministic ordering of updates for each key. Paravon’s leaderless architecture allows it
to scale horizontally to thousands of nodes and operate efficiently behind standard load balancers
using round‑robin or similar strategies. Any node can serve client requests, and the system naturally
distributes load without requiring coordination or a central authority.

![QuickStart](./doc/quickstart.gif)

Paravon offers:

* Deterministic, conflict‑free convergence
* Leaderless replication across a consistent‑hashing ring
* Hinted handoff with preserved update ordering
* Horizontal scalability to large clusters
* Compatibility with standard load balancers
* A binary‑safe, self‑describing protocol
* Mandatory mutual TLS for all communication
* A lightweight, deterministic update model


## Installing

Install with `pip` or your favorite PyPI package manager.

```bash
pip install paravon
```

**Python 3.14+** is required.

Run the following to test Paravon output on your terminal:

```bash
paravon --help
paractl --help
```

## Quickstart

### 1. Download the certificate generator

```bash
curl -LO https://github.com/mistersouls/paravon/releases/download/0.0.1.dev0/generate-cert.sh
chmod +x generate-cert.sh
```

This script use **OpenSSL CLI**

### 2. Generate certificates

#### CA

```bash
./generate-cert.sh ca
```

#### Server certificate (node-1)

```bash
./generate-cert.sh server nodes/node-1 node-1 "" 127.0.0.1
```

#### Client certificate (user‑1)

```bash
./generate-cert.sh client clients/user-1 user-1
```

### 3. Create the server configuration

```bash
mkdir -p nodes/node-1/data

echo '
node:
  id: node-1

server:
  api:
    port: 2001
  peer:
    port: 6001
  tls:
    cafile: ./ca/ca.crt
    keyfile: ./nodes/node-1/server.key
    certfile: ./nodes/node-1/server.crt

storage:
  data_dir: ./nodes/node-1/data
' > nodes/node-1/paranode.yaml
```

A full configuration template is available here:
https://raw.githubusercontent.com/mistersouls/paravon/refs/heads/main/paranode.yaml

### 4. Create the client configuration

```bash
mkdir -p clients/user-1

echo '
current-context: dev
contexts:
  dev:
    server: 127.0.0.1:2001
    tls:
      ca: ./ca/ca.crt
      cert: ./clients/user-1/client.crt
      key: ./clients/user-1/client.key
    client:
      client_id: user-1
      timeout: 5s
' > clients/user-1/paraconf.yaml
```

A full configuration template is available here:
https://raw.githubusercontent.com/mistersouls/paravon/refs/heads/main/paraconf.yaml


### 5. Start the server

```bash
paravon --config nodes/node-1/paranode.yaml
```

Paravon listens on:

* 2001 → Client
* 6001 → Admin

To change ports, edit paranode.yaml.

### 6. Interact with the node

#### Status

```bash
paractl --paraconf ./clients/user-1/paraconf.yaml \
    --server 127.0.0.1:6001 \
    admin node-status
```

#### Create

```bash
paractl --paraconf ./clients/user-1/paraconf.yaml \
    put users/souls '{"username": "souls"}'
```

#### Read

```bash
paractl --paraconf ./clients/user-1/paraconf.yaml \
      get users/souls
```

#### Delete

```bash
paractl --paraconf ./clients/user-1/paraconf.yaml \
    delete users/souls
```

#### Read again

```bash
paractl --paraconf ./clients/user-1/paraconf.yaml \
    get users/souls
```
