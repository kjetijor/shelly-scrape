#! /usr/bin/env python3
"""Shit shelly plug scraper"""

from atexit import register as register_atexit
from argparse import ArgumentParser
from collections import namedtuple
from dataclasses import dataclass
from typing import Mapping, List, Tuple
from pathlib import Path
from time import sleep

import os
import signal
import sys
import yaml

from prometheus_client import CollectorRegistry, Gauge, write_to_textfile
import requests

LabelType = namedtuple("LabelType", "name value")


@dataclass
class PlugConfig:
    """Shelly plug metrics config"""
    name: str
    location: str
    device: str


@dataclass
class ShellyscrapeConfig:
    """Shellyscrape config"""
    commonlabels: Mapping[str, str]
    plugs: Mapping[str, PlugConfig]


OUTDIR = Path("/var/lib/prometheus/node-exporter")


def shelly_scrape_config(path: Path) -> ShellyscrapeConfig:
    """Load plug config from path"""
    plugs = {}
    with open(path, "r", encoding="utf-8") as fh:
        plug_defs = yaml.load(fh, Loader=yaml.SafeLoader)
    commonlabels = {}
    try:
        for (key, value) in plug_defs["commonlabels"].items():
            if not isinstance(key, str):
                raise ValueError(f"invalid key {key}")
            if not isinstance(value, str):
                raise ValueError(f"invalid value {value}")
            commonlabels[key] = value
    except KeyError:
        pass
    for (name, plug_cfg) in plug_defs["plugs"].items():
        plugs[name] = PlugConfig(
            name=name if "name" not in plug_cfg.keys() else plug_cfg["name"],
            location=plug_cfg["location"],
            device=plug_cfg["device"],
        )
    return ShellyscrapeConfig(
        commonlabels=commonlabels,
        plugs=plugs
    )


@dataclass
class PlugApparentEnergy:
    """Apparent energy usage"""
    total: float
    by_minute: List[float]
    minute_ts: int


@dataclass
class PlugMetrics:
    """Shelly plug metrics"""
    id: int
    source: str
    output: bool
    apower: float
    voltage: float
    current: float
    aenergy: PlugApparentEnergy
    temperature_c: float
    temperature_f: float


def plug_metrics_from_dict(d: dict) -> PlugMetrics:
    """Construct PlugMetrics from dict"""
    return PlugMetrics(
        id=d["id"],
        source=d["source"],
        output=d["output"],
        apower=d["apower"],
        voltage=d["voltage"],
        current=d["current"],
        aenergy=PlugApparentEnergy(
            total=d["aenergy"]["total"],
            by_minute=d["aenergy"]["by_minute"],
            minute_ts=d["aenergy"]["minute_ts"]
        ),
        temperature_c=d["temperature"]["tC"],
        temperature_f=d["temperature"]["tF"]
    )


def scrape(host: str) -> PlugMetrics:
    """Scrape shellyplug on host into registry reg, with labelmap"""
    rsp = requests.get(f"http://{host}/rpc/Switch.GetStatus?id=0", timeout=5)
    if rsp.status_code < 200 or rsp.status_code >= 300:
        raise ValueError("bad response")
    data = rsp.json()
    return plug_metrics_from_dict(data)


def create_metrics(
        commonlabels: Mapping[str, str],
        measurements: Mapping[str, Tuple[PlugConfig, PlugMetrics]]
    ) -> CollectorRegistry:
    """Create metrics from measurements with commonlabels, returns prometheus CollectorRegistry"""
    labelnames, labelvalues = zip(
        *commonlabels.items()) if len(commonlabels.keys()) > 0 else ([], [])
    reg = CollectorRegistry()
    labelnames = list(labelnames) + ["scrapehost", "location", "device", "name"]
    apower = Gauge("shellyplug_apparent_power",
                   "apparent power", labelnames, registry=reg)
    voltage = Gauge("shellyplug_voltage", "voltage", labelnames, registry=reg)
    current = Gauge("shellyplug_current", "current", labelnames, registry=reg)
    totenergy = Gauge("shellyplug_apparent_energy_usage_total",
                      "total energy usage in WH", labelnames, registry=reg)
    aenergy = Gauge(
        "shellyplug_apparent_energy_usage", "apparent energy usage in mWH",
        labelnames + ["bucket"],
        registry=reg)
    temperature = Gauge("shellyplug_temperature", "temperature",
                        labelnames + ["unit"], registry=reg)
    for (scrapehost, (config, plugmetrics)) in measurements.items():
        plugvalues = list(
            labelvalues) + [scrapehost, config.location, config.device, config.name]
        apower.labels(*plugvalues).set(plugmetrics.apower)
        voltage.labels(*plugvalues).set(plugmetrics.voltage)
        current.labels(*plugvalues).set(plugmetrics.current)
        totenergy.labels(*plugvalues).set(plugmetrics.aenergy.total)
        for (i, bucket) in enumerate(plugmetrics.aenergy.by_minute):
            aenergy.labels(*plugvalues, f"minute_{i}").set(bucket)
        temperature.labels(*plugvalues, "c").set(plugmetrics.temperature_c)
        temperature.labels(*plugvalues, "f").set(plugmetrics.temperature_f)
    return reg


def write_metrics(metricsdir: Path, reg: CollectorRegistry,
                  filename="shellyscrape.prom"):
    """write metrics from registry reg to metricsdir/filename"""
    pid = os.getpid()
    tmpfn = metricsdir / f".{filename}.{pid}.tmp"
    outfn = metricsdir / filename
    write_to_textfile(str(tmpfn), reg)
    Path(tmpfn).rename(outfn)


class DoneHandler:
    """wrapping shenanigans for signal handlers"""

    def __init__(self):
        self._done = False

    def __call__(self, _signum, _frame):
        self._done = True

    def done(self) -> bool:
        """return done status"""
        return self._done


def gen_cleanup(dirname: Path, name: str):
    """generate cleanup function"""
    def cleanup():
        for f in dirname.glob(f".{name}.*.tmp"):
            f.unlink()
        (dirname / name).unlink(missing_ok=True)
    return cleanup


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="Shelly plug scraper",
        description="Scrape shelly plugs and write out prometheus metrics"
    )
    parser.add_argument("--outdir", "-o", type=Path,
                        default=OUTDIR, help="output directory")
    parser.add_argument(
        "--config", "-c", type=Path,
        default="/etc/shelly-scrape/shelly-scrape.yaml",
        help="plug labels file")
    parser.add_argument(
        "--filename", "-f", type=str, default="shellyscrape.prom",
        help="output filename")
    parser.add_argument(
        "--add-plug", "-P", type=str, action="append", default=[],
        help="plug hostnames")
    parser.add_argument("--interval", "-i", type=int,
                        default=3, help="interval in seconds")
    parser.add_argument("--no-cleanup", action="store_true",
                        default=False, help="don't cleanup on exit")
    parser.add_argument("--once", action="store_true",
                        default=False, help="run once and exit")
    parser.add_argument("--debug", action="store_true",
                        default=False, help="debug mode")

    args = parser.parse_args()
    if not args.no_cleanup:
        register_atexit(gen_cleanup(args.outdir, args.filename))
    print(f"Loading plugs config from {args.config}", file=sys.stderr)
    scrapecfg = shelly_scrape_config(args.config)
    print(
        f"Plugs with config {','.join(scrapecfg.plugs.keys())}," \
        f" ad-hoc plugs {','.join(args.add_plug)}",
        file=sys.stderr
    )
    targets: Mapping[str, PlugConfig] = {}
    for (plug, cfg) in scrapecfg.plugs.items():
        targets[plug] = cfg
    for plug in args.add_plug:
        if plug in targets:
            print(f"Skipping {plug}, already in config", file=sys.stderr)
            continue
        targets[plug] = PlugConfig(name=plug, location="adhoc", device="adhoc")
    if len(targets.keys()) == 0:
        print("No plugs given", file=sys.stderr)
        sys.exit(1)
    donehandler = DoneHandler()
    signal.signal(signal.SIGTERM, donehandler)
    signal.signal(signal.SIGINT, donehandler)
    print(
        f"Starting with interval {args.interval}s, " \
        f"outputting metrics to {args.outdir}/{args.filename}",
        file=sys.stderr)
    while not donehandler.done():
        metrics: Mapping[str, Tuple[PlugConfig, PlugMetrics]] = {}
        for (plug, cfg) in targets.items():
            try:
                m = scrape(plug)
                metrics[plug] = (cfg, m)
            except requests.exceptions.ConnectionError as conerr:
                print(
                    f"Failed to scprape {plug}, failed to connect with {conerr}",
                    file=sys.stderr)
        registry = create_metrics(scrapecfg.commonlabels, metrics)
        write_metrics(args.outdir, registry, args.filename)
        if args.once:
            print("Exiting because once", file=sys.stderr)
            sys.exit(1)
        sleep(args.interval)
    print("Shutting down", file=sys.stderr)
