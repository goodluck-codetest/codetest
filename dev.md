Choose your familiar language to implement an algorithm execution engine. 
1. Framework design
  - core data structures, class definition of market data, order etc
  - order management component (OMS)
  - strategy interfaces, e.g. OnTick, OnOrderResponse etc
2. Trading algo implemention
  - 2 execution algos: TWAP and Pair-trading 
  - target completion with certain limits, e.g. end-time, max participation
  - risk management, e.g, imbalance fills, market volatile moves
  - execution optimization, e.g, order slicing
3. Design document
  - including but not limited to architecture overview, modularization, persistence, user interface etc.
