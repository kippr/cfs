# cfs
Cash flow simulator using Python PEP 492 coroutines


Simple simulation library that allows you to write cashflow generator functions using PEP 492 async syntax, which can yield cashflows and await future periods:

```python
  async def first(clock, balances):
      yield 50, 'A', 'B', ''
      yield 50, 'A', 'B', ''
      await clock.tick(years=1)
      yield 300, 'A', 'B', ''
  
  
  async def second(clock, balances):
      await clock.until(date(2020, 1, 1))
      yield 100, 'A', 'B', ''
  
  
  sim = Simulation(first, second, start_date=date(2019, 6, 1), end_date=date(2030, 6, 1)).run()
  bals = sim.balances
  expect(bals.loc[date(2019, 6, 1), 'B']) == 100
  expect(bals.loc[date(2020, 1, 1), 'B']) == 200
  expect(bals.loc[date(2020, 6, 1), 'B']) == 500
```
