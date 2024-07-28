TB-Collect
-----------------

TB-Collect implements efficient garbage collection algorithms for Non-Volatile Memory OLTP Engines.

Experiment Environment
------------

Experiments are conducted on a server equipped with an Intel Xeon Gold 5218R CPU (20 physical cores @2.10GHz and 27.5 MB LLC) and 256 GB DDR4 DRAM, running Ubuntu 20.04.3 LTS. Each core supports two hardware threads, resulting in a total of 40 threads. There are 768 GB (128x6 GB) Intel Optane DC Persistent Memory NVDIMMs in the system. Optane PM has two accessibility modes, Memory Mode and APP Direct mode. We choose APP Direct mode for easy mapping of DRAM and NVM into the same virtual address space. We deploy file systems in fs-dax mode on NVM, followed by employing libpmem within PMDK to establish mappings between NVM files and a process's virtual memory. To ensure data persistence to NVM, we utilize \textit{clwb} and \textit{sfence}. The entire codebase is developed in C/C++ and compiled using gcc 7.5.0.

Experimental Results
-------------

- **Transaction Performance**
    - TPCC Performance: https://github.com/w1397800/TB-Collect/wiki/TPCC-Performance
    - YCSB Performance: https://github.com/w1397800/TB-Collect/wiki/YCSB-Performance
- **Chain Consolidation Performance:**
    - https://github.com/w1397800/TB-Collect/wiki/Chain-Consolidation-Performance
- **Performance over time**
    - Space Size Increase Over 10 Minutes: https://github.com/w1397800/FIR/wiki/Checkpoint-Speed-&-Checkpoint-Scale
    - Throughput over 200 Seconds: https://github.com/w1397800/TB-Collect/wiki/Space-Size-Increase-Over-10-Minutes
