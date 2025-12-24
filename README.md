# `raft` - Sans I/O Implementation of the Raft Algorithm

A zero-dependency, pure Rust implementation of the Raft consensus algorithm
[without I/O](https://www.firezone.dev/blog/sans-io).

Official paper: [Raft Consensus Algorithm](https://raft.github.io/raft.pdf)

## Why?

Raft is at the core of modern distributed systems.
[Kubernetes](https://kubernetes.io/), [etcd](https://etcd.io/), and many other
critical infrastructure components use Raft as their consensus algorithm.

I learn and understand best by building things. I’ve found that edge cases and
corner cases are often revealed during development. Reading the paper isn't
enough for me.

## Contributing

If you want to contribute to `raft` to add a feature or improve the code contact
me at [alexandre@negrel.dev](mailto:alexandre@negrel.dev), open an
[issue](https://github.com/negrel/raft/issues) or make a
[pull request](https://github.com/negrel/raft/pulls).

## :stars: Show your support

Please give a :star: if this project helped you!

[![buy me a coffee](https://github.com/negrel/.github/blob/master/.github/images/bmc-button.png?raw=true)](https://www.buymeacoffee.com/negrel)

## :scroll: License

MIT © [Alexandre Negrel](https://www.negrel.dev/)
