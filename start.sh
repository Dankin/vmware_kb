#!/bin/bash
# VMware KB系统启动脚本

echo "启动VMware KB展示系统..."
echo "访问 http://localhost:8000 查看系统"
echo "访问 http://localhost:8000/docs 查看API文档"
echo ""
echo "按 Ctrl+C 停止服务"
echo ""

uvicorn main:app --reload --host 0.0.0.0 --port 8000

