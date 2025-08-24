import unittest
from expecter import expect
import pandas as pd
from datetime import date

from cfs.simulation import Simulation
from cfs.simulation import Clock
from cfs.simulation import Balances
from cfs.simulation import assert_accounts
from cfs.simulation import InvalidWaitTime
from cfs.simulation import InvalidCashFlowYielded
from cfs.simulation import InvalidAccount
from cfs.simulation import FailedToAwaitClock

import logging
logging.basicConfig(level=logging.TRACE)


class WhenSimulatingCashFlows():

    def should_generate_cashflows_in_simplest_case(self):
        async def cfs(clock, balances):
            yield 100, 'debit', 'credit', '1st'
            yield 200, 'debit', 'credit', '2nd'
            yield 300, 'debit', 'credit', '3rd'
        sim = Simulation(cfs, start_date=date(2019, 6, 1), end_date=date(2030, 6, 1)).run()
        expect(sim.current_balances['debit']) == -600

    def should_interleave_generators(self):
        async def first(clock, balances):
            yield 100, 'A', 'B', 'first'
            yield 300, 'A', 'B', 'third'
        async def second(clock, balances):
            yield 200, 'A', 'C', 'second'
            yield 400, 'A', 'C', 'last'
        sim = Simulation(first, second, start_date=date(2019, 6, 1), end_date=date(2030, 6, 1)).run()
        expect(sim.cashflows['amount'].tolist()) == [100, 200, 300, 400]

    def should_respect_clock(self):
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

    def should_enforce_time_moves_only_forward(self):
        async def first(clock, balances):
            yield 100, 'A', 'B', 'Meh'
            await clock.until(date(2010, 1,1))
        sim = Simulation(first, start_date=date(2019, 6, 1), end_date=date(2030, 6, 1))
        with expect.raises(InvalidWaitTime):
            sim.run()

    def should_end_simulation_if_clock_advances_past_end_date(self):
        async def test(clock, balances):
            while True:
                await clock.next_calendar_year_end()
                yield 100, 'A', 'B', 'Meh'
        sim = Simulation(test, start_date=date(2019, 1, 1), end_date=date(2020, 1, 1))
        sim.run()
        expect(sim.current_period) is None

    def should_be_able_to_handle_multiple_waits_in_a_row(self):
        async def test(clock, balances):
            # if for example there is an if clause around yield?
            await clock.next_calendar_year_end()
            await clock.next_calendar_year_end()
            await clock.next_calendar_year_end()
            yield 100, 'A', 'B', 'Finally ready'
        sim = Simulation(test, start_date=date(2019, 1, 1), end_date=date(2025, 1, 1)).run()
        cf = sim.cashflows
        result = cf.loc[cf['date'] == date(2021, 12, 31), 'amount'].tolist()
        expect(result) == [100]

    def should_enforce_generators_await_clock(self):
        # doesn't work because call to async wait method without await means it won't be run
        # is there a way to see which futures are doing nothing and assert failure?
        async def badly_written_generator(clock, balances):
            yield 100, 'A', 'B', 'ok'
            clock.tick(years=1)
            yield 100, 'A', 'B', 'ok'
        sim = Simulation(badly_written_generator, start_date=date(2019, 6, 1), end_date=date(2030, 6, 1))
        with expect.raises(FailedToAwaitClock):
            sim.run()

    def should_check_only_valid_cashflows_yielded(self):
        async def badly_written_generator(clock, balances):
            yield 100, 'A', 'B'  # forgot desc
        sim = Simulation(badly_written_generator, start_date=date(2019, 6, 1), end_date=date(2030, 6, 1))
        with expect.raises(InvalidCashFlowYielded):
            sim.run()


class WhenAssertingWhichAccountsAreBeingUsed():

    def should_enforce_only_registered_accounts_are_accessed(self):
        @assert_accounts('registered_acct1', 'registered_acct2')
        async def badly_written_generator(clock, balances):
            balances['registered_acct1']
            balances['forgotten_acct']
            yield 100, 'registered_acct1', 'registered_acct2', 'Should not get this far'
        sim = Simulation(badly_written_generator, start_date=date(2019, 6, 1), end_date=date(2030, 6, 1))
        with expect.raises(InvalidAccount):
            sim.run()

    def should_enforce_cash_only_flows_between_with_registered_accounts(self):
        @assert_accounts('registered_acct')
        async def badly_written_generator(clock, balances):
            yield 100, 'registered_acct', 'forgotten_acct', 'should die because forgotten not registered'
        sim = Simulation(badly_written_generator, start_date=date(2019, 6, 1), end_date=date(2030, 6, 1))
        with expect.raises(InvalidAccount):
            sim.run()


class FakeSimulation(object):
    def __init__(self, start_date=None, current_balances=None, **kwargs):
        self.start_date = start_date
        self.current_balances = current_balances

    def _logger(self, *args):
        return None


def create_clock(start_date=date(2010, 1, 1)):
    sim = FakeSimulation(start_date=start_date)
    return Clock(sim, None)


class WhenWorkingWithClocks():

    def should_be_able_to_hop_to_year_end(self):
        clock = create_clock()
        clock.simulation.current_period = date(2010, 6, 30)
        next(clock.next_calendar_year_end())
        expect(clock.waiting_for) == date(2010, 12, 31)

    def should_be_able_to_hop_to_year_end_when_at_it(self):
        clock = create_clock()
        clock.simulation.current_period = date(2010, 12, 31)
        next(clock.next_calendar_year_end())
        expect(clock.waiting_for) == date(2011, 12, 31)


class WhenAccessingBalances():

    def should_be_able_to_get_net_balance_across_multiple_accts(self):
        bals = pd.DataFrame([dict(cash=30, investments=70, other=200)]).loc[0]
        sim = FakeSimulation(current_balances=bals)
        accts = ('cash', 'investments')
        balances = Balances(sim, accts)
        expect(balances.sum(accts)) == 100
