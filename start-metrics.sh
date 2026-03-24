#!/bin/bash
# 启动独立的SLSMetrics监控服务脚本

# 参数1: 配置文件所在目录路径（可选）
CONFIG_DIR=${1:-"src/main/resources"}
echo "Starting SLSMetrics monitoring service with config dir: $CONFIG_DIR..."

# 检查target目录是否存在
if [ ! -d "target" ] || [ ! -d "target/lib" ] || [ ! -d "target/classes" ]; then
  echo "Error: Please build the project first using 'mvn clean package'"
  exit 1
fi

# 启动监控服务
java -cp "target/lib/*:target/classes" org.apache.hadoop.sls.SLSMetrics $CONFIG_DIR