# Phoenix

Phoenix is a fault-tolerant distibuted low-latency scheduler based on [Sparrow](https://cs.stanford.edu/~matei/papers/2013/sosp_sparrow.pdf) implemented in Go. It uses [Zookeeper](https://github.com/apache/zookeeper) to implement group membership for worker nodes. This enables Phoenix to handle worker failures and recover incomplete jobs, unlike Sparrow.
