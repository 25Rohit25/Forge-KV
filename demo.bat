@echo off
echo Building and running ForgeKV 8-Step Interactive Demo...
call mvn clean compile -DskipTests
if %errorlevel% neq 0 (
    echo Build failed.
    exit /b %errorlevel%
)
java -cp "target/classes;target/dependency/*" com.forgekv.client.ForgeKVCLI demo
