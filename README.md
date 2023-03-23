# InfluxQL

[![CI](https://github.com/CeresDB/influxql/actions/workflows/CI.yml/badge.svg)](https://github.com/CeresDB/influxql/actions/workflows/CI.yml)

InfluxQL parser and planner written in Rust.

In order to merge upstream easily, the directory is the same with IOx, main components are:
- `influxdb_influxql_parser`, parser
- `iox_query_influxql`, planner

Other components are used to make `cargo build` successfully.

# Acknowledgment
Initial version forked from [InfluxDB IOx](https://github.com/influxdata/influxdb_iox/tree/08ef689d2196c79cdd94680f51460929c2f406be).
