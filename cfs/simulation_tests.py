import unittest
from expecter import expect
import pandas as pd
from datetime import date

from cfs.simulation import Simulation
from cfs.simulation import Clock
from cfs.simulation import Accounts
from cfs.simulation import Account
from cfs.simulation import InvalidWaitTime
from cfs.simulation import InvalidCashFlowYielded
from cfs.simulation import InvalidAccount
from cfs.simulation import FailedToAwaitClock

import logging
logging.basicConfig(level=logging.TRACE)


class WhenSimulatingCashFlows():

    def should_generate_cashflows_in_simplest_case(self):
        async def cfs(sim):
            yield sim.cf(amount=100, src='debit', dst='credit', desc='1st')
            yield sim.cf(200, 'debit', 'credit', '2nd')
            yield sim.cf(300, 'debit', 'credit', '3rd')
        sim = Simulation(start_date=date(2019, 6, 1), accts=dict(debit=0, credit=0)).add(cfs).run()
        #kp: todo: how to deal with accts?
        expect(sim.accts.current_balances['debit']) == -600

    def should_interleave_generators(self):
        async def first(sim):
            yield sim.cf(100, 'A', 'B', 'first')
            yield sim.cf(300, 'A', 'B', 'third')
        async def second(sim):
            yield sim.cf(200, 'A', 'C', 'second')
            yield sim.cf(400, 'A', 'C', 'last')
        sim = Simulation(first, second, 
                         start_date=date(2019, 6, 1), 
                         end_date=date(2030, 6, 1),
                         accts=dict(A=0, B=0, C=0),
                         ).run()
        expect(sim.accts.journals['amount'].tolist()) == [0, 0, 0, 100, 200, 300, 400]

    def should_respect_clock(self):
        async def first(sim):
            yield sim.cf(50, 'A', 'B')
            yield sim.cf(50, 'A', 'B')
            await sim.clock.tick(years=1)
            yield sim.cf(300, 'A', 'B')
        async def second(sim):
            await sim.clock.until(date(2020, 1, 1))
            yield sim.cf(100, 'A', 'B')
        sim = Simulation(first, second, 
                         start_date=date(2019, 6, 1), 
                         end_date=date(2030, 6, 1),
                         accts=dict(A=0, B=0),
                         ).run()
        bals = sim.accts.balances_by_date
        expect(bals.loc[date(2019, 6, 1), 'B']) == 100
        expect(bals.loc[date(2020, 1, 1), 'B']) == 200
        expect(bals.loc[date(2020, 6, 1), 'B']) == 500

    def should_enforce_time_moves_only_forward(self):
        async def first(sim):
            yield sim.cf(100, 'A', 'B', 'Meh')
            await sim.clock.until(date(2010, 1,1))
        sim = Simulation(first, start_date=date(2019, 6, 1), end_date=date(2030, 6, 1), accts=dict(A=0, B=0))
        with expect.raises(InvalidWaitTime):
            sim.run()

    def should_end_simulation_if_clock_advances_past_end_date(self):
        async def test(sim):
            while True:
                await sim.clock.next_calendar_year_end()
                yield sim.cf(100, 'A', 'B', 'Meh')
        sim = Simulation(test, start_date=date(2019, 1, 1), end_date=date(2019, 6, 30), accts=dict(A=0, B=0))
        sim.run()
        expect(sim.accts.journals['amount'].tolist()) == [0, 0]

    def should_be_able_to_handle_multiple_waits_in_a_row(self):
        async def test(sim):
            # if for example there is an if clause around yield?
            await sim.clock.next_calendar_year_end()
            await sim.clock.next_calendar_year_end()
            await sim.clock.next_calendar_year_end()
            yield sim.cf(100, 'A', 'B', 'Finally ready')
        sim = Simulation(test, start_date=date(2019, 1, 1), accts=dict(A=0, B=0)).run()
        cf = sim.accts.journals
        result = cf.loc[cf['date'] == date(2021, 12, 31), 'amount'].tolist()
        expect(result) == [100]

    def should_enforce_generators_await_clock(self):
        # doesn't work because call to async wait method without await means it won't be run
        # is there a way to see which futures are doing nothing and assert failure?
        async def badly_written_generator(sim):
            yield sim.cf(100, 'A', 'B', 'ok')
            sim.clock.tick(years=1)
            yield sim.cf(100, 'A', 'B', 'ok')
        sim = Simulation(badly_written_generator, 
                         start_date=date(2019, 6, 1), 
                         end_date=date(2030, 6, 1), 
                         accts=dict(A=0, B=0))
        with expect.raises(FailedToAwaitClock):
            sim.run()

    def should_check_only_valid_cashflows_yielded(self):
        async def badly_written_generator(sim):
            yield 'hello mum'
        sim = Simulation(badly_written_generator, start_date=date(2019, 6, 1), end_date=date(2030, 6, 1))
        with expect.raises(InvalidCashFlowYielded):
            sim.run()


class WhenAssertingWhichAccountsAreBeingUsed():

    def should_enforce_only_registered_accounts_are_accessed(self):
        async def badly_written_generator(sim):
            should_fail = sim.accts.current_balances['forgotten_acct']
            yield 'meh ' # without a yield in body you get GeneratorExhaustedError without anything running
        sim = Simulation(badly_written_generator, start_date=date(2019, 6, 1))
        with expect.raises(KeyError):
            sim.run()

    def should_enforce_cash_only_flows_between_registered_accounts(self):
        async def badly_written_generator(sim):
            yield sim.cf(100, 'registered_acct', 'forgotten_acct', 'should die because forgotten not registered')
        sim = Simulation(badly_written_generator, start_date=date(2019, 6, 1), accts=[
            Account(name='registered_acct', initial=1000),
        ])
        with expect.raises(InvalidAccount):
            sim.run()

    def should_enforce_each_account_is_registered_only_once(self):
        def badly_configured_simulation():
            sim = Simulation(start_date=date(2019, 6, 1), accts=[
                Account(name='registered_acct', initial=1000),
                Account(name='registered_acct', initial=500),
            ])
            sim.run()
        with expect.raises(ValueError):
            badly_configured_simulation()


def create_clock(start_date=date(2010, 1, 1)):
    class FakeSimulation(object):
        def __init__(self, start_date=None):
            self.start_date = start_date

        def _logger(self, *args):
            return None

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


class WhenAccessingAccounts():

    def should_be_able_to_get_net_balance_across_multiple_accts(self):
        accts = Accounts(start_date=date(2025, 8, 24), accounts=[
            Account(name='cash', initial=30),
            Account(name='investments', initial=70),
            Account(name='other', initial=200),

        ])
        expect(accts.sum(('cash', 'investments'))) == 100
