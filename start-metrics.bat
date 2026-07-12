@echo off
REM 启动独立的SLSMetrics监控服务脚本

REM 参数1: 配置文件所在目录路径（可选）
set CONFIG_DIR=%1
if "%CONFIG_DIR%"=="" set CONFIG_DIR=src/main/resources

echo Starting SLSMetrics monitoring service with config dir: %CONFIG_DIR%...

REM 检查target目录是否存在
if not exist "target\lib" (
  echo Error: Please build the project first using 'mvn clean package'
  exit /b 1
)

if not exist "target\classes" (
  echo Error: Please build the project first using 'mvn clean package'
  exit /b 1
)

REM 启动监控服务
java -cp "target/lib/*;target/classes" org.apache.hadoop.sls.metrics.SLSMetrics %CONFIG_DIR%