# Algorithm

This document describes our improved algorithm for Raft.

1. At the beginning, we let each node $n_i \in \{n_1, ..., n_N\}$ act as the leader for $T_i$ seconds, where $T_i$ are initialized to a hyperparameter $\mathcal{T}$ for all $i$.

2. We maintain an exponential moving average (EMA) of the message latency $t_i$ for node $n_i$, and after $n_i$ acts as the leader, we update the duration of its leadership $T_i$ based on the ratio of EMA to its latency.
The higher the ratio, the higher duration they can get in the next leadership.
This mechanism allows nodes with lower latency to have a proportionally longer duration as leaders, as a higher ratio translates to a longer leadership period.

3. After each node acts as the leader once, we choose the next leader probabilistically: with the probability of $p$ we choose the node $n_i$ with the highest $T_i$, and with the probability of $1 - p$ we choose the node randomly.
This randomness injects an element of chance into the leader selection process, providing an opportunity for nodes that initially experienced longer network delays but possess better network characteristics to emerge as leaders.
In our work, we choose to set $p = 0.6$.

4. If the leader $n_i$ demonstrates a lengthy latency than its history, we early abort its leadership.
We choose to abort its leadership if $t'_i < k \cdot t_i$, where $k$ is a hyperparameter and $t'_i$ is its current latency.
This way, we can prevent a leader with degrading performance from ruling for a long duration $T_i$.

5. When a follower $n_p$ becomes the candidate and starts a new term (typically because the leader $n_L$ crashes), a follower $n_i$ does not answer right after it receives a $\mathtt{RequestVote}$ RPC.
Instead, it waits for $\mathcal{W}$ seconds to see whether there is another candidate $n_q$ such that $T_q > T_p$.
This approach also gives the node demonstrating better performance an edge in the election process.

## Transition function of $T$

The transition function is $T' := T \cdot m$, where $m$ is a multiplier. We want $m \in [0.5, 2]$.
We can compute $m$ from the ratio (of the EMA of latency across all nodes to the current latency of some node) using this function: $f(x) = \frac{1}{1+e^{-kx}} \cdot \frac{3}{2} + 0.5$, where $k$ is a hyperparameter (temparature).
