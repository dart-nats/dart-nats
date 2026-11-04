# Developer Guide: Environment Setup & Testing

This guide contains everything you need to set up your local environment, run the NATS test servers, execute the test suite, and contribute to the `dart_nats` package.

---

## 🛠️ Prerequisites

Before you start, make sure you have the following tools installed:

1. **Dart SDK**: Version `>=2.15.0 <4.0.0` is required. Verify your version:
   ```bash
   dart --version
   ```
2. **Docker & Docker Compose**: Needed to run the containerized NATS test servers. Verify installations:
   ```bash
   docker --version
   docker-compose --version
   ```
3. **OpenSSL**: Required to generate local SSL/TLS certificates for secure connection testing. Verify installation:
   ```bash
   openssl version
   ```

---

## 🚀 Local Environment Setup

### 1. Fetch Dependencies
Navigate to the project root directory and fetch the necessary package dependencies:
```bash
dart pub get
```

### 2. Generate SSL/TLS Test Certificates
The test suite validates secure TLS and WSS (WebSocket Secure) connections. You must generate self-signed certificates and place them in the `test/config/` directory.

Run the following command from the project root:
```bash
openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 \
  -out test/config/server-cert.pem \
  -keyout test/config/server-key.pem \
  -subj "/C=US/ST=State/L=City/O=Organization/OU=OrgUnit/CN=localhost"
```

---

## 🐳 Starting the NATS Test Infrastructure

The testing suite relies on a local, multi-instance NATS topology. Docker Compose configures these instances with different authentication schemes and protocols.

### Start the Servers
Spin up all test containers in the background:
```bash
docker-compose up -d
```

This starts the following containerized NATS services:
* **`nats`** (Port `4222`): Standard connection (TCP) & WebSockets with JetStream enabled (`-js`).
* **`nats-jwt`** (Port `4223`): NATS JWT Authentication.
* **`nats-token`** (Port `4224`): Token Authentication.
* **`nats-user`** (Port `4225`): Username/Password Authentication.
* **`nats-nkey`** (Port `4226`): NKEY Authentication.
* **`nats-jwt2`** (Port `4227`): Alternative NATS JWT Authentication setup.
* **`nats-tls`** (Port `4443` & `8443`): Secure TLS connection and WSS.

To verify that the NATS containers are running, execute:
```bash
docker ps --filter "name=nats"
```

### Stop the Servers
When done testing, shut down the containers to free up resources:
```bash
docker-compose down
```

---

## 🧪 Running the Test Suite

Because several tests publish and subscribe to the same topics on the same shared local servers, running tests concurrently will cause test pollution and unexpected failures.

### Execute Tests Sequentially
You **must** run the test runner with 1 thread (single job concurrency) by specifying `-j 1` or `--concurrency=1`:

```bash
dart test -j 1
```

---

## 📦 Releasing

1. Set the new `version:` in `pubspec.yaml` and add a matching `## <version>` section to `CHANGELOG.md`.
2. Merge, then push a `v<version>` tag. CI checks that the tag, `pubspec.yaml` and the changelog agree, publishes to pub.dev, and creates the GitHub release.

The version the client reports to servers in CONNECT comes from `lib/src/version.dart`. The publish job writes it from `pubspec.yaml` just before publishing, so you don't have to. To update the checked-in copy too, run `dart run tool/write_version.dart`.

---

## 🔍 Troubleshooting

### 1. `NatsException: Connection refused` or `SocketException`
* **Cause**: The NATS containers are not running, or their ports are bound by another process.
* **Solution**: Run `docker ps` to ensure the NATS containers are active. If ports are in use by other local services, stop those services first.

### 2. `Cannot connect to the Docker daemon`
* **Cause**: Docker is not started on your machine.
* **Solution**: Open Docker Desktop (on macOS/Windows) or run `sudo systemctl start docker` (on Linux) and retry.

### 3. File System permission issues with TLS certificates
* **Cause**: The generated `server-cert.pem` and `server-key.pem` files are missing or inaccessible to the NATS docker container mount.
* **Solution**: Ensure they exist in `test/config/` and that the permissions allow read access.
