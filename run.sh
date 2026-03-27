#!/bin/bash
# 启动股票雷达
cd "$(dirname "$0")"
source .venv/bin/activate
python main.py
