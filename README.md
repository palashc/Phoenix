# Phoenix

Phoenix is a fault-tolerant distibuted low-latency scheduler based on [Sparrow]([https://cs.stanford.edu/~matei/papers/2013/sosp_sparrow.pdf](https://cs.stanford.edu/~matei/papers/2013/sosp_sparrow.pdf)) implemented in Go. It uses [zookeeper](https://github.com/apache/zookeeper) to implement group membership for worker nodes. This enables Phoenix to handle worker failures and avoid incomplete jobs, unlike Sparrow.
