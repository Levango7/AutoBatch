@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo [1/1] 运行 AutoBatch（生成示例数据 + 五阶段处理）...
python -m src.pipeline --config config\pipeline.json
if errorlevel 1 (
  echo [FAIL] 流水线执行失败，请查看 run\latest.json 定位失败阶段
  exit /b 1
)
echo [OK] 流水线执行完成，运行结果见 run\latest.json
