import time
import logging
from typing import Dict, Any

logger = logging.getLogger("metrics")

class MetricsTracker:
    def __init__(self):
        self.active_streams: int = 0
        self.total_streams_served: int = 0
        self.failed_streams: int = 0
        self.total_bytes_transferred: int = 0
        self.recent_ttfb: list = []  # last 50 TTFB measurements in seconds
        self.recent_speeds: list = []  # last 50 speeds in MB/s

    def stream_started(self):
        self.active_streams += 1
        self.total_streams_served += 1

    def stream_finished(self, bytes_sent: int, duration_sec: float, ttfb_sec: float):
        if self.active_streams > 0:
            self.active_streams -= 1
        
        self.total_bytes_transferred += bytes_sent

        if ttfb_sec > 0:
            self.recent_ttfb.append(ttfb_sec)
            if len(self.recent_ttfb) > 50:
                self.recent_ttfb.pop(0)

        if duration_sec > 0 and bytes_sent > 0:
            speed_mbps = (bytes_sent / (1024 * 1024)) / duration_sec
            self.recent_speeds.append(speed_mbps)
            if len(self.recent_speeds) > 50:
                self.recent_speeds.pop(0)
            
            logger.info(
                f"Stream finished | Transferred: {bytes_sent / (1024*1024):.2f} MB | "
                f"Duration: {duration_sec:.2f}s | TTFB: {ttfb_sec*1000:.1f}ms | "
                f"Speed: {speed_mbps:.2f} MB/s | Active: {self.active_streams}"
            )

    def stream_failed(self):
        if self.active_streams > 0:
            self.active_streams -= 1
        self.failed_streams += 1

    def get_summary(self) -> Dict[str, Any]:
        avg_ttfb_ms = (
            sum(self.recent_ttfb) / len(self.recent_ttfb) * 1000
            if self.recent_ttfb
            else 0.0
        )
        avg_speed_mbps = (
            sum(self.recent_speeds) / len(self.recent_speeds)
            if self.recent_speeds
            else 0.0
        )

        return {
            "active_streams": self.active_streams,
            "total_streams_served": self.total_streams_served,
            "failed_streams": self.failed_streams,
            "total_data_gb": round(self.total_bytes_transferred / (1024**3), 3),
            "avg_ttfb_ms": round(avg_ttfb_ms, 1),
            "avg_speed_mb_s": round(avg_speed_mbps, 2),
        }

metrics = MetricsTracker()
