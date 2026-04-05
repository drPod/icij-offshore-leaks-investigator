# Supply Chain Simulator API

Backend-only Jac API for simulating npm package ecosystem compromise propagation.

## Endpoints

Public functions:
- `build_graph(packages)`
- `reset_graph()`
- `get_graph()`
- `infect_package(package_name)`
- `assess_compromise()`

Public walkers:
- `BuildGraph`
- `Infect`
- `Assess`
- `Reset`
- `GetGraph`

## Run

```bash
jac start main.jac
```
