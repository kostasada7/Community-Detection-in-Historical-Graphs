# Community-Detection-in-Historical-Graphs
This repository provides a community detection framework tailored for temporal graphs. We introduce two algorithms designed for temporal networks:

t-iWCC: an interval-based version of the distributed WCC algorithm.

t-wWCC: a weighted-edge adaptation of the same method.

In addition, we propose two variants of the Label Propagation Algorithm (LPA):

t-iLPA, which operates on interval-based edge information.

t-wLPA, which leverages weighted edges to capture temporal dynamics.

Each branch contains the code corresponding to its respective method. The data and intervals methodology branch includes the interval-processing code used for the Orkut dataset, as well as two real-world datasets that share a uniform time interval across all edges.

The t-iWCC and t-wWCC implementations are based on the framework available at https://github.com/TariqAbughofa/incremental_distributed_wcc.

To run t-iWCC and t-wWCC, execute their main files. For the remaining algorithms, simply run their standalone script files, as each consists of a single script.
