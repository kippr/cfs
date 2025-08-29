# cfs
Cash flow simulator using Python PEP 492 coroutines


Simple simulation library that allows you to write cashflow generator functions using PEP 492 async syntax, which can yield cashflows and await future periods:

```python
    accts = Accounts({'acct_a': 1000, 'acct_b': 0})

    async def first(sim):
      yield sim.cf(amount=50, src='acct_a', dst='acct_b', desc='Move 50 from A to B')
      yield sim.cf(50, 'acct_a', 'acct_b')
      await sim.clock.tick(years=1)
      yield sim.cf(300, 'acct_a', 'acct_b')

    async def second(sim):
      await sim.clock.until(date(2020, 1, 1))
      yield sim.cf(100, 'acct_a', 'acct_b')


    sim = Simulation(first, second, accts=accts, start_date=date(2019, 6, 1), end_date=date(2030, 6, 1)).run()
    bals = sim.accts.balances_by_date
    expect(bals.loc[date(2019, 6, 1), 'acct_b']) == 100
    expect(bals.loc[date(2020, 1, 1), 'acct_b']) == 200
    expect(bals.loc[date(2020, 6, 1), 'acct_b']) == 500
```
