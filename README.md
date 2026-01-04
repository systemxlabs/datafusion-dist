# datafusion-dist
![License](https://img.shields.io/badge/license-MIT-blue.svg)
[![Crates.io](https://img.shields.io/crates/v/datafusion-dist.svg)](https://crates.io/crates/datafusion-dist)
[![Docs](https://docs.rs/datafusion-dist/badge.svg)](https://docs.rs/datafusion-dist/latest/datafusion_dist/)

A distributed streaming execution library for [Apache DataFusion](https://datafusion.apache.org/).

## Overview

datafusion-dist enables distributed query execution for DataFusion, allowing you to scale analytical workloads across multiple nodes.

## Features

- Distributed streaming execution for DataFusion queries
- Pluggable cluster management (PostgreSQL support)
- Pluggable network layer (Tonic support)
- Extensible planner for custom planning stages
- Extensible scheduler for custom task scheduling

## Project Structure

```
datafusion-dist/
├── dist/                 # Core distributed execution library
├── clusters/
│   └── postgres/         # PostgreSQL-based cluster management
├── networks/
│   └── tonic/            # gRPC network layer using Tonic
└── integration-tests/    # Integration test suite
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.