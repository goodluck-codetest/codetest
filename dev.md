Choose your familiar language to implement automatic trading stragies and its platform. Use yahoo finance api (or yfinance https://pypi.org/project/yfinance/ ) to download the past 5 years’ SPX 500 stock daily price to complete your system. Data prepareation part can be different language like Python.

1. Framework design
  - core data structures, class definition of market data, order etc
  - order management component (OMS)
  - strategy interfaces, e.g. OnTick, OnOrderResponse etc
    
2. Strategy implemention
  - algorithm execution engine
    - implement TWAP (for each stock) and pair-trading algos (for stock pairs)
    - target completion with certain limits, e.g. end-time, max participation
    - risk management, e.g, imbalance fills, market volatile moves
    - execution optimization, e.g, order slicing
  - beta hedger
    - stock universe is SPX 500, and hedging insturment is SPY ETF
    - calculate 90 days' rolling beta for each stock benchmarking to SPY.
    - beta exposure = beta * position. total beta expo = sum of all stocks
    - place hedging orders to hedge net beta exposure under given risk
    - include open orders into consideration, which can be stock order or hedging order itself
      
3. Design document
  - include but not limited to architecture overview, modularization, persistence, user interface etc.
