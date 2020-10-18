ring
---

# Build

- Setup [spack](https://github.com/spack/spack)

- Install and load [mochi-thallium](https://xgitlab.cels.anl.gov/sds/thallium)


```bash
$ spack install mochi-thallium
$ spack load -r mochi-thallium
$ g++ -g -std=c++14 `pkg-config --libs thallium` -pthread ring.cpp -o ring.out
```

# Usage

To start ring.

```bash
$ ./ring.out
```
To join to exists ring.

```bash
$ ./ring.out [one of address which consist ring]
```

For examples,

```bash
$ ./ring.out
Server running at address ofi+tcp;ofi_rxm://123.45.67.89:12345
...
```

```bash
$ ./ring.out 'ofi+tcp;ofi_rxm://123.45.67.89:12345'
...
```


# Demo

![demo](./demo.gif)