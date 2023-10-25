Choose your familiar language to implement automatic trading stragies and its platform. You can fake some data to support the implementation.
1. Framework design
  - core data structures, class definition of market data, order etc
  - order management component (OMS)
  - strategy interfaces, e.g. OnTick, OnOrderResponse etc
2. Strategy implemention
  - algos engine
    - implement TWAP and pair-trading algos
    - target completion with certain limits, e.g. end-time, max participation
    - risk management, e.g, imbalance fills, market volatile moves
    - execution optimization, e.g, order slicing
  - beta hedger
    - each stock has its own beta to indice benchmark, beta exposure = beta * position. total beta expo = sum of all stocks
    - place index future orders to hedge net beta exposure under given risk
    - include open orders into consideration, which can be stock order or hedging order itself
3. Design document
  - include but not limited to architecture overview, modularization, persistence, user interface etc.
