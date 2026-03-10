@echo off
echo 正在打包网络代理服务器轻量版...
echo.

REM 检查Python是否可用
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo 错误: Python未安装或不在PATH中
    pause
    exit /b 1
)

echo 安装轻量版依赖包...
pip install -r requirements_lite.txt
if %errorlevel% neq 0 (
    echo 警告: 某些依赖包安装失败，继续打包...
)

echo.
echo 正在打包成exe文件...
pyinstaller build_lite.spec
if %errorlevel% neq 0 (
    echo 错误: 打包失败
    pause
    exit /b 1
)

echo.
echo 打包完成！
echo exe文件位置: dist\网络代理服务器-轻量版.exe
echo.
echo 按任意键打开输出目录...
pause >nul
start explorer dist

exit /b 0