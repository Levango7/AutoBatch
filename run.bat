@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo [1/2] 运行 AutoBatch（生成示例数据 + 五阶段处理）...
python -m src.pipeline --config config\pipeline.json
if errorlevel 1 (
  echo [FAIL] 流水线执行失败，请查看 run\latest.json 定位失败阶段
  exit /b 1
)
echo [2/2] 刷新看板数据（dashboard\data.js）...
python dashboard\build_data.py
if errorlevel 1 (
  echo [WARN] 看板数据刷新失败，dashboard.html 将展示上次刷新的数据
) else (
  echo [OK] 完成。运行结果见 run\latest.json；打开 dashboard\dashboard.html 查看看板
)
