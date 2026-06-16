#!/usr/bin/env python3
"""A lightweight web monitor for the icecc scheduler.

Connects to icecc-scheduler's binary monitor protocol and exposes the
cluster state via WebSocket, plus a static HTML dashboard.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import struct
import time
from pathlib import Path

import aiohttp
from aiohttp import web


# ---------------------------------------------------------------------------
# Protocol constants (from icecc services/comm.h)
# ---------------------------------------------------------------------------

# Highest icecc monitor protocol version this client understands.
# The scheduler may down-negotiate to an older version, but claiming the
# current upstream value lets us connect to newer schedulers without
# editing this constant after every icecc release.
SUPPORTED_PROTOCOL_VERSION = 44
MAX_MSG_SIZE = 10 * 1024 * 1024

MSG_TYPES = {
    ord("R"): "mon_login",
    ord("S"): "get_cs",
    ord("T"): "job_begin",
    ord("U"): "job_done",
    ord("V"): "local_job_begin",
    ord("W"): "stats",
    ord("O"): "local_job_done",
}


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def decode_string(data: bytes) -> tuple[str, bytes]:
    """Decode an icecc length-prefixed NUL-terminated string."""
    if len(data) < 4:
        raise ValueError("string length truncated")
    length = struct.unpack(">I", data[:4])[0]
    if length == 0:
        return "", data[4:]
    if len(data) < 4 + length:
        raise ValueError("string payload truncated")
    payload = data[4 : 4 + length]
    if payload[-1:] != b"\x00":
        raise ValueError("string not NUL-terminated")
    return payload[:-1].decode("utf-8", errors="replace"), data[4 + length :]


def encode_string(s: str) -> bytes:
    """Encode a string for the icecc wire format."""
    encoded = s.encode("utf-8") + b"\x00"
    return struct.pack(">I", len(encoded)) + encoded


def parse_message(data: bytes) -> dict | None:
    """Parse one complete framed message from the scheduler.

    Returns ``None`` for message types that are known to exist in newer
    protocol versions but are not handled by this monitor, so the caller
    can skip them instead of dropping the connection.
    """
    if len(data) < 8:
        raise ValueError("message too short")
    length = struct.unpack(">I", data[:4])[0]
    payload = data[4 : 4 + length]
    if len(payload) < 4:
        raise ValueError("payload too short")
    raw_type, rest = struct.unpack(">I", payload[:4])[0], payload[4:]
    msg_type = MSG_TYPES.get(raw_type)
    if msg_type is None:
        logging.debug("Ignoring unknown scheduler message type %d", raw_type)
        return None

    if msg_type == "stats":
        hostid = struct.unpack(">I", rest[:4])[0]
        statmsg, _ = decode_string(rest[4:])
        stats = {}
        for line in statmsg.splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                stats[key] = value
        return {"type": "stats", "hostid": hostid, "stats": stats}

    if msg_type == "get_cs":
        filename, rest = decode_string(rest)
        lang = struct.unpack(">I", rest[:4])[0]
        job_id, clientid = struct.unpack(">II", rest[4:12])
        return {
            "type": "get_cs",
            "job_id": job_id,
            "clientid": clientid,
            "filename": filename,
        }

    if msg_type == "job_begin":
        job_id, stime, hostid = struct.unpack(">III", rest[:12])
        return {
            "type": "job_begin",
            "job_id": job_id,
            "stime": stime,
            "hostid": hostid,
        }

    if msg_type == "local_job_begin":
        hostid, job_id, stime = struct.unpack(">III", rest[:12])
        filename, _ = decode_string(rest[12:])
        return {
            "type": "local_job_begin",
            "job_id": job_id,
            "stime": stime,
            "hostid": hostid,
            "filename": filename,
        }

    if msg_type in ("job_done", "local_job_done"):
        job_id = struct.unpack(">I", rest[:4])[0]
        return {"type": "job_done", "job_id": job_id}

    if msg_type == "mon_login":
        return {"type": "mon_login"}

    raise ValueError(f"unsupported message type {msg_type}")


# ---------------------------------------------------------------------------
# State model
# ---------------------------------------------------------------------------


class State:
    """Mutable in-memory snapshot of the icecc cluster."""

    def __init__(self):
        self.nodes: dict[int, dict] = {}
        self.jobs: dict[int, dict] = {}
        self.completed_jobs = 0

    def _inc_current_jobs(self, hostid: int | None) -> None:
        if hostid is None:
            return
        node = self.nodes.setdefault(hostid, {"id": hostid})
        node["current_jobs"] = node.get("current_jobs", 0) + 1

    def _dec_current_jobs(self, hostid: int | None) -> None:
        if hostid is None:
            return
        node = self.nodes.get(hostid)
        if node is None:
            return
        node["current_jobs"] = max(0, node.get("current_jobs", 0) - 1)

    def update(self, msg: dict) -> None:
        if msg["type"] == "stats":
            self._update_node(msg["hostid"], msg["stats"])
        elif msg["type"] == "get_cs":
            self.jobs[msg["job_id"]] = {
                "id": msg["job_id"],
                "client_id": msg["clientid"],
                "filename": msg["filename"],
                "state": "pending",
                "stime": msg.get("stime", time.time()),
            }
        elif msg["type"] == "local_job_begin":
            job = self.jobs.get(msg["job_id"])
            if job is None:
                job = {"id": msg["job_id"]}
                self.jobs[msg["job_id"]] = job
            old_state = job.get("state")
            old_host = job.get("hostid")
            if old_state == "local" and old_host is not None and old_host != msg["hostid"]:
                self._dec_current_jobs(old_host)
            job.update(
                {
                    "hostid": msg["hostid"],
                    "filename": msg["filename"],
                    "state": "local",
                    "stime": msg["stime"],
                }
            )
            if old_state != "local":
                self._inc_current_jobs(msg["hostid"])
        elif msg["type"] == "job_begin":
            job = self.jobs.get(msg["job_id"])
            if job is None:
                job = {"id": msg["job_id"], "filename": "", "client_id": 0}
                self.jobs[msg["job_id"]] = job
            old_state = job.get("state")
            old_server = job.get("server_id")
            if old_state == "compiling" and old_server is not None and old_server != msg["hostid"]:
                self._dec_current_jobs(old_server)
            job["state"] = "compiling"
            job["server_id"] = msg["hostid"]
            job["stime"] = msg["stime"]
            if old_state != "compiling":
                self._inc_current_jobs(msg["hostid"])
        elif msg["type"] == "job_done":
            job = self.jobs.get(msg["job_id"])
            if job is not None:
                state = job.get("state")
                if state == "local":
                    self._dec_current_jobs(job.get("hostid"))
                elif state == "compiling":
                    self._dec_current_jobs(job.get("server_id"))
                del self.jobs[msg["job_id"]]
                self.completed_jobs += 1

    @staticmethod
    def _to_int(value: str | None) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    @staticmethod
    def _to_float(value: str | None) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except ValueError:
            return None

    def _update_node(self, hostid: int, stats: dict[str, str]) -> None:
        node = self.nodes.get(hostid, {})
        node.update(
            {
                "id": hostid,
                "name": stats.get("Name", node.get("name", "")),
                "ip": stats.get("IP", node.get("ip", "")),
                "platform": stats.get("Platform", node.get("platform", "")),
                "max_jobs": self._to_int(stats.get("MaxJobs")) or node.get("max_jobs", 0),
                "load": self._to_int(stats.get("Load")) or node.get("load", 0),
                "load_avg_1": self._to_int(stats.get("LoadAvg1")) or node.get("load_avg_1", 0),
                "load_avg_5": self._to_int(stats.get("LoadAvg5")) or node.get("load_avg_5", 0),
                "load_avg_10": self._to_int(stats.get("LoadAvg10")) or node.get("load_avg_10", 0),
                "free_mem": self._to_int(stats.get("FreeMem")) or node.get("free_mem", 0),
                "speed": self._to_float(stats.get("Speed")) or node.get("speed", 0.0),
                "version": self._to_int(stats.get("Version")) or node.get("version", 0),
                "features": stats.get("Features", node.get("features", "")),
                "no_remote": stats.get("NoRemote", "").lower() == "true",
                "current_jobs": node.get("current_jobs", 0),
                "last_seen": time.time(),
            }
        )
        self.nodes[hostid] = node

    def prune_stale_nodes(self, max_age: float = 60.0) -> None:
        """Remove nodes that have not sent stats for a while.

        icecc daemons can reconnect with a different hostid; without pruning
        the old entry stays visible as an offline duplicate forever.
        """
        now = time.time()
        stale_ids = [
            hostid
            for hostid, node in self.nodes.items()
            if now - node.get("last_seen", 0) > max_age
        ]
        for hostid in stale_ids:
            del self.nodes[hostid]

    def snapshot(self) -> dict:
        return {
            "nodes": self.nodes,
            "jobs": self.jobs,
            "stats": {
                "node_count": len(self.nodes),
                "active_jobs": len(self.jobs),
                "completed_jobs": self.completed_jobs,
            },
        }


def _pack_version(version: int) -> bytes:
    """Pack a protocol version the way icecc expects (little-endian 4 bytes)."""
    return struct.pack("<I", version)


def _unpack_version(data: bytes) -> int:
    """Unpack a protocol version sent by icecc (little-endian 4 bytes)."""
    return struct.unpack("<I", data)[0]


# ---------------------------------------------------------------------------
# Scheduler connector
# ---------------------------------------------------------------------------


class SchedulerConnector:
    """Persistent asyncio connection to icecc-scheduler."""

    def __init__(
        self,
        host: str,
        port: int,
        state: State,
        protocol_version: int = SUPPORTED_PROTOCOL_VERSION,
    ):
        self.host = host
        self.port = port
        self.state = state
        self.protocol_version = protocol_version
        self.agreed_protocol_version: int | None = None
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._closed = False

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

        # Protocol negotiation (matches icecc MsgChannel handshake):
        #   1. We send the maximum version we support.
        #   2. Scheduler replies with the version it wants to use
        #      (min of both sides).
        #   3. We echo that agreed version back.
        #   4. Scheduler confirms by echoing the agreed version again.
        self.writer.write(_pack_version(self.protocol_version))
        await self.writer.drain()

        server_version_data = await self.reader.readexactly(4)
        server_version = _unpack_version(server_version_data)
        agreed_version = min(self.protocol_version, server_version)

        self.writer.write(_pack_version(agreed_version))
        await self.writer.drain()

        echoed_version_data = await self.reader.readexactly(4)
        echoed_version = _unpack_version(echoed_version_data)
        if echoed_version != agreed_version:
            raise ConnectionError(
                f"protocol negotiation failed: expected {agreed_version}, got {echoed_version}"
            )

        self.agreed_protocol_version = agreed_version
        logging.info(
            "Connected to scheduler %s:%d (protocol version %d)",
            self.host,
            self.port,
            agreed_version,
        )

        # MonLoginMsg: 4-byte big-endian length (value 4) + 4-byte big-endian type 'R'.
        self.writer.write(struct.pack(">II", 4, ord("R")))
        await self.writer.drain()

    async def run(self) -> None:
        backoff = 1
        while not self._closed:
            try:
                await self.connect()
                backoff = 1
                while not self._closed:
                    length_data = await self.reader.readexactly(4)
                    length = struct.unpack(">I", length_data)[0]
                    if length > MAX_MSG_SIZE:
                        raise ValueError(f"message too large: {length}")
                    payload = await self.reader.readexactly(length)
                    msg = parse_message(length_data + payload)
                    if msg is not None:
                        self.state.update(msg)
            except (ConnectionError, asyncio.IncompleteReadError) as exc:
                logging.warning("Scheduler connection lost: %s", exc)
            except Exception:
                logging.exception("Scheduler protocol error")
            finally:
                await self._close_connection()
            if self._closed:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _close_connection(self) -> None:
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            self.writer = None
            self.reader = None

    async def close(self) -> None:
        self._closed = True
        await self._close_connection()


# ---------------------------------------------------------------------------
# WebSocket / HTTP server
# ---------------------------------------------------------------------------


class MonitorServer:
    """Broadcasts state snapshots to browser clients via Server-Sent Events."""

    def __init__(self, state: State, refresh_interval: float = 1.0):
        self.state = state
        self.clients: set[web.StreamResponse] = set()
        self.refresh_interval = refresh_interval

    async def events_handler(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            status=200,
            reason="OK",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )
        await response.prepare(request)
        # Send an initial snapshot immediately upon connection.
        await self._send_snapshot(response)
        self.clients.add(response)
        try:
            # Keep the connection alive until the client disconnects.
            while not response.task.done():
                await asyncio.sleep(1)
        finally:
            self.clients.discard(response)
        return response

    async def _send_snapshot(self, response: web.StreamResponse) -> None:
        payload = json.dumps(self.state.snapshot())
        try:
            await response.write(f"data: {payload}\n\n".encode("utf-8"))
        except (ConnectionResetError, BrokenPipeError):
            pass

    async def broadcast(self) -> None:
        if not self.clients:
            return
        payload = json.dumps(self.state.snapshot())
        dead: set[web.StreamResponse] = set()
        for response in self.clients:
            try:
                await response.write(f"data: {payload}\n\n".encode("utf-8"))
            except (ConnectionResetError, BrokenPipeError):
                dead.add(response)
            except Exception:
                logging.exception("Failed to send to client")
                dead.add(response)
        self.clients -= dead


async def index_handler(request: web.Request) -> web.Response:
    index_path = Path(__file__).with_name("index.html")
    if not index_path.exists():
        return web.Response(
            status=404,
            text="index.html not found next to icecc_monitor.py",
        )
    return web.FileResponse(index_path)


async def config_handler(request: web.Request) -> web.Response:
    server = request.app["server"]
    if request.method == "GET":
        return web.json_response(
            {"refresh_interval": server.refresh_interval}
        )
    try:
        body = await request.json()
        interval = float(body.get("refresh_interval", server.refresh_interval))
        if interval < 0.2 or interval > 60:
            raise ValueError("refresh_interval must be between 0.2 and 60")
        server.refresh_interval = interval
        logging.info("Refresh interval set to %.1fs", interval)
        return web.json_response({"refresh_interval": server.refresh_interval})
    except Exception as exc:
        return web.json_response({"error": str(exc)}, status=400)


async def periodic_broadcast(server: MonitorServer) -> None:
    while True:
        await asyncio.sleep(server.refresh_interval)
        server.state.prune_stale_nodes()
        await server.broadcast()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Icecc scheduler web monitor")
    parser.add_argument("--scheduler-host", default="localhost")
    parser.add_argument("--scheduler-port", type=int, default=8765)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument(
        "--protocol-version",
        type=int,
        default=SUPPORTED_PROTOCOL_VERSION,
        help=(
            "maximum icecc protocol version to advertise during handshake "
            f"(default: {SUPPORTED_PROTOCOL_VERSION})"
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    state = State()
    connector = SchedulerConnector(
        args.scheduler_host,
        args.scheduler_port,
        state,
        protocol_version=args.protocol_version,
    )
    server = MonitorServer(state)

    app = web.Application()
    app["server"] = server
    app.router.add_get("/", index_handler)
    app.router.add_get("/events", server.events_handler)
    app.router.add_get("/api/config", config_handler)
    app.router.add_post("/api/config", config_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, args.host, args.port)
    await site.start()

    scheduler_task = asyncio.create_task(connector.run())
    broadcast_task = asyncio.create_task(periodic_broadcast(server))

    logging.info("Dashboard: http://%s:%d", args.host, args.port)
    logging.info("SSE endpoint: http://%s:%d/events", args.host, args.port)

    try:
        await asyncio.gather(scheduler_task, broadcast_task)
    except asyncio.CancelledError:
        pass
    finally:
        await connector.close()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
