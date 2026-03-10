@echo off
echo 正在打包网络代理服务器...
echo.

REM 检查Python是否可用
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo 错误: Python未安装或不在PATH中
    pause
    exit /b 1
)

echo 安装依赖包...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo 错误: 依赖包安装失败
    pause
    exit /b 1
)

echo.
echo 正在打包成exe文件...
pyinstaller build.spec
if %errorlevel% neq 0 (
    echo 错误: 打包失败
    pause
    exit /b 1
)

echo.
echo 打包完成！
echo exe文件位置: dist\网络代理服务器.exe
echo.
echo 按任意键打开输出目录...
pause >nul
start explorer dist

exit /b 0