#!/bin/bash
set -e
echo "Building and running ForgeKV 8-Step Interactive Demo..."
mvn clean compile -DskipTests
java -cp "target/classes:target/dependency/*" com.forgekv.client.ForgeKVCLI demo
