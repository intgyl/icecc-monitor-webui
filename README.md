# Icecc Web Monitor

一个基于浏览器的 icecc scheduler 监控工具，通过 SSE 实时展示集群节点、任务状态和负载信息。

## 功能特性

- **实时数据**：通过 SSE 连接 icecc scheduler，自动推送最新集群状态
- **自动刷新**：支持 0.5s / 1s / 2s / 5s / 10s / 30s 多档刷新频率
- **多视图切换**：
  - 摘要视图：集群概览 + 节点列表 + 活跃任务
  - 节点列表：完整节点信息
  - 任务视图：活跃任务详情
  - 甘特视图：任务时间线
  - 节点详情：单击节点进入详情页
- **负载显示**：按 icecream-sundae 风格显示编译槽位利用率（current_jobs / max_jobs）
- **暗色主题**：采用 web_uart 风格的暗色界面

## 环境要求

- Python 3.10+
- `aiohttp`

## 安装依赖

```bash
pip install aiohttp
```

## 运行

```bash
python icecc_monitor.py
```

默认连接 `localhost:8765` 的 scheduler，并在 `0.0.0.0:8080` 提供 Web 服务。

打开浏览器访问：

```
http://localhost:8080
```

## 命令行参数

```bash
python icecc_monitor.py --scheduler-host localhost --scheduler-port 8765 --host 0.0.0.0 --port 8080
```

| 参数 | 说明 | 默认值 |
|---|---|---|
| `--scheduler-host` | icecc scheduler 主机 | `localhost` |
| `--scheduler-port` | icecc scheduler 端口 | `8765` |
| `--host` | HTTP 服务监听地址 | `0.0.0.0` |
| `--port` | HTTP 服务端口 | `8080` |
| `--protocol-version` | 握手时声明支持的最高协议版本 | 内置最新版本 |

