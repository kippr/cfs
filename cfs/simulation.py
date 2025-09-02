from collections import namedtuple
from dataclasses import dataclass
from enum import Enum
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import time
import types
import functools
import logging
import inspect
logging.addLevelName(5, 'TRACE')
logging.TRACE = 5
WAITING = type("Waiting", (), dict(__repr__=lambda self: "WAITING"))()
INITIAL = 'initial'
INITIAL_BALANCE_DESCRIPTION = 'Initial balance'


CashFlow = namedtuple('CashFlow', ('txn_id', 'date', 'amount', 'from_acct', 'to_acct', 'description', 'generator'))


class AcctType(Enum):
    ASSET = 'asset'
    LIABILITY = 'liability'
    INCOME = 'income'
    EXPENSE = 'expense'
    EQUITY = 'equity'


@dataclass
class Account:
    name: str
    initial: float = 0.0
    description: str = None
    type: AcctType = None
    category: str = None


class Simulation(object):
    """ Cash flow generators generate cash flows.. duh. But sometimes they want to wait for clock to progress. Might
    be nice to look at asyncio to use coroutines to interleave these things rather than magic yield statements
    (and needing to use yield from)?
    """
    id_fountain = None
    last_period = None
    @dataclass
    class GeneratorState:
        generator: any
        iter_cashflows: any
        simctx: 'SimContext'
        logger: 'SimulationLoggerAdapter'
        async_gen: any = None

        @property
        def clock(self):
            return self.simctx.clock

    def __init__(self, *cashflow_generators, accts=None, start_date=None, end_date=None):
        if not end_date:
            end_date = start_date + relativedelta(years=5)
        assert start_date < end_date, 'Start date must be < end_date'
        self.logger = self._logger(self.__class__, 'Main')
        self.start_date, self.current_period, self.end_date = start_date, start_date, end_date
        if not accts or isinstance(accts, dict):
            accts = Accounts(accts)
        self.accts = accts
        self.generators = tuple(cashflow_generators)

    def add(self, *cashflow_generators):
        self.generators = self.generators + tuple(cashflow_generators)
        return self

    def _prepare_run(self):
        if self.id_fountain is not None:
            raise GeneratorExhausted("Simulation already run?")
        self.id_fountain = iter(range(1, 10000000000))
        self.accts._prepare(self.start_date)
        self.last_period = False
        self.generators = tuple(self._setup_generators())
        self.logger.info("Initialized cashflow generators: %s",
                         [f'{g.generator.__name__}' for g in self.generators])

    def _setup_generators(self):
        for cashflow_generator_fn in self.generators:
            if not inspect.isasyncgenfunction(cashflow_generator_fn):
                raise InvalidGenerator(f"Cashflow generator '{cashflow_generator_fn.__name__}' "
                    "is not an async generator function")
            clock = Clock(self, self._logger(cashflow_generator_fn, 'Clock'))
            simctx = SimContext(
                self.accts, 
                clock, 
                self.id_fountain, 
                cashflow_generator_fn,
                self._logger(cashflow_generator_fn, 'SimContext')
            )
            iter_cashflows = cashflow_generator_fn(simctx)
            logger = self._logger(cashflow_generator_fn, 'Cashflows')
            yield self.GeneratorState(
                generator=cashflow_generator_fn,
                iter_cashflows=iter_cashflows,
                logger=logger,
                simctx=simctx,
            )

    def _logger(self, generator, label):
        logger = logging.getLogger(f'{generator.__name__}:{label}')
        return SimulationLoggerAdapter(self, logger)

    def _period_cashflows(self):
        for g in self.generators[:]:
            if g.clock.ready:
                try:
                    cf = _next(g)
                except GeneratorExhausted:
                    g.logger.info("Exhausted.. removing.")
                    self.generators = tuple(x for x in self.generators if g != x)
                    g.simctx.close()
                except NotReadyAfterAll:
                    g.logger.debug("No cfs yet after all.. Two awaits in a row.")
                else:
                    if cf == WAITING:
                        g.clock._wait_was_awaited()
                        g.logger.trace('will wait until %s', g.clock.waiting_for)
                    elif isinstance(cf, CashFlow):
                        g.simctx._cf_was_yielded()
                        cf = self.accts._assert_accounts_are_valid(cf)
                        g.logger.trace('Transfer %s from %s to %s: "%s"', cf.amount, cf.from_acct, cf.to_acct, cf.description)
                        yield cf
                    else:
                        msg = f'Expected a cashflow (amount, from, to, description) but got "{cf}"'
                        g.logger.error(msg)
                        raise InvalidCashFlowYielded(msg)

    def _maybe_advance_period(self, must_advance):
        if not self.generators:
            raise StopSimulation('All cashflow generators exhausted. Stopping simulation')
        new_period = min((g.clock.waiting_for for g in self.generators if g.clock.waiting_for))
        if must_advance and new_period == self.current_period:
            raise StopSimulation( "No cashflows this period and no generator waiting for future period.. stopping early")
        if self.current_period == new_period:
            self.logger.trace("Staying on %s to check for more cashflows", self.current_period)
            return False
        else:
            if self.last_period:
                raise StopSimulation("Was on last period and advance requested: stopping.")
            self.current_period = new_period
            if self.current_period == self.end_date:
                self.logger.info("Advancing to *last* period: %s", self.current_period)
                self.last_period = True
            elif self.current_period > self.end_date:
                raise StopSimulation("Advanced past *last* period (%s), stopping", self.end_date)
            else:
                self.logger.debug("Advancing to %s", self.current_period)
            return True

    def run(self):
        self._prepare_run()
        cfs_for_period = []
        while True:
            for cf in self._period_cashflows():
                cfs_for_period.append(cf)
            try:
                period_advanced = self._maybe_advance_period(must_advance=not cfs_for_period)
            except StopSimulation as e:
                self.accts.append(cfs_for_period)
                self.logger.info(str(e))
                self.current_period = None
                for g in self.generators[:]:
                    g.simctx.close()
                break
            else:
                if period_advanced:
                    self.accts.append(cfs_for_period)
                    cfs_for_period = []
        return self

    def to_excel(self, filename):
        """ Write data that can be consumed by cashflows-viz.twb """
        writer = pd.ExcelWriter(filename)
        bals = self.balances.unstack().reset_index()
        bals.columns = ['acct', 'date', 'balance']
        bals.to_excel(writer, 'balances', index=False)
        credits = self.cashflows.copy()
        credits.columns = ['txn_id', 'date', 'from_acct', 'to_acct', 'amount', 'description']
        debits = credits.copy()
        swap = debits['from_acct'].copy()
        debits['from_acct'] = debits['to_acct']
        debits['to_acct'] = swap
        debits['amount'] = debits['amount'] * -1.
        journals = credits.append(debits).sort_values('txn_id')
        journals.to_excel(writer, 'journals', index=False)
        writer.save()


class SimContext:
    
    def __init__(self, accounts, clock, id_fountain, generator, logger):
        self.accts = accounts
        self.clock = clock
        self.id_fountain = id_fountain
        self.generator = generator.__name__
        self._unyielded_cfs: int = 0
        self.logger = logger

    def cf(self, amount, src, dst, desc=None):
        """Create a cashflow"""
        txn_id = next(self.id_fountain)
        current_period = self.clock.current_period
        self._unyielded_cfs += 1
        return CashFlow(txn_id, current_period, amount, src, dst, desc, self.generator)

    def _cf_was_yielded(self):
        self._unyielded_cfs -= 1
        self.clock._cf_was_yielded()

    def close(self):
        self._assert_all_cfs_yielded()
        self.id_fountain = None
        self.clock = None
        self.accts = None

    def _assert_all_cfs_yielded(self):
        if self._unyielded_cfs > 0:
            msg = f"{self._unyielded_cfs} unyielded cashflow(s) remaining when generator ended"
            self.logger.error(msg)
            raise FailedToYieldCashFlow(msg)


def _next(state):
    # grab the next cash flow from the async generator or else a clock waiting event
    try:
        if state.async_gen is None:
            state.async_gen = state.iter_cashflows.__anext__()
    except AttributeError as e:
        raise NotReadyAfterAll()
    try:
        waiting = state.async_gen.send(None)
    except StopIteration as si:
        state.async_gen = None
        return si.value
    except StopAsyncIteration as sai:
        raise GeneratorExhausted()
    else:
        return waiting


def enforce_awaited(wrapped):
    @functools.wraps(wrapped)
    def checked(clock, *args, **kwargs):
        clock._awaiting_clock_wait = wrapped.__name__
        return wrapped(clock, *args, **kwargs)
    return checked


class Clock(object):

    def __init__(self, simulation, clock_logger):
        self.simulation = simulation
        self._waiting_for = simulation.start_date
        self.logger = clock_logger
        self._awaiting_clock_wait = False
        self._unyielded_cfs = 0

    @property
    def current_period(self):
        return self.simulation.current_period

    @property
    def start_date(self):
        return self.simulation.start_date

    @property
    def end_date(self):
        return self.simulation.end_date

    @property
    def ready(self):
        self.logger.trace("Ready? waiting for: %s, will run now: %s",
                          self.waiting_for, self.waiting_for == self.current_period)
        return self.waiting_for == self.current_period

    @property
    def waiting_for(self):
        return self._waiting_for

    @waiting_for.setter
    def waiting_for(self, waiting_for):
        #kp: todo: consider asserting this only when it is awaited?
        # should be easy and avoids clock having to use temp vars below in `until_day`
        if waiting_for < self.current_period:
            msg = f'Requesting wait until {waiting_for}, but that has already passed; currently at {self.current_period}'
            self.logger.error(msg)
            raise InvalidWaitTime(msg)
        self._waiting_for = waiting_for

    @property
    def elapsed(self):
        return relativedelta(self.current_period, self.simulation.start_date)

    @enforce_awaited
    @types.coroutine
    def tick(self, **kwargs):
        date = self.current_period + relativedelta(**kwargs)
        yield from self.until(date)

    @enforce_awaited
    @types.coroutine
    def until(self, date):
        self.waiting_for = date
        yield WAITING

    @enforce_awaited
    @types.coroutine
    def next_calendar_year_end(self):
        """Wait until next 31 Dec"""
        self.waiting_for = datetime.date(self.current_period.year, 12, 31)
        if self.waiting_for == self.current_period:
            self.waiting_for = datetime.date(self.current_period.year + 1, 12, 31)
        yield WAITING

    @enforce_awaited
    @types.coroutine
    def until_day(self, day):
        waiting_for = datetime.date(self.current_period.year, self.current_period.month, day)
        if waiting_for <= self.current_period:
            month = self.current_period.month + 1
            year = self.current_period.year
            if month > 12:
                month = 1
                year += 1
            waiting_for = datetime.date(year, month, day)
        self.waiting_for = waiting_for
        yield WAITING


    def _wait_was_awaited(self):
        self._awaiting_clock_wait = False

    def _cf_was_yielded(self):
        if self._awaiting_clock_wait:
            func_name = self._awaiting_clock_wait
            msg = f"You called clock.{func_name} without awaiting the result (use 'await clock.{func_name}(..)' instead"
            self.logger.error(msg)
            raise FailedToAwaitClock(msg)


class Accounts():

    def __init__(self, accts=None):
        """Hold the accounts for a sim.. Accounts can be created as a dict of name:initial_balance,
        or using the `add(<details>)` method"""
        if accts:
            self._accounts = {k: Account(name=k, initial=v) for k, v in accts.items()}
            for a in self._accounts.values():
                self._add_as_property(a)
        else:
            self._accounts = {}

    def _prepare(self, start_date):
        initial_cfs = [CashFlow(0, start_date, acct.initial or 0, INITIAL, acct.name, INITIAL_BALANCE_DESCRIPTION, INITIAL)
            for acct in self._accounts.values()]
        if INITIAL not in self._accounts:
            self.add(INITIAL, category='External', type=AcctType.EQUITY)
        self._journals = JournalEntries(initial_cfs)

    def add(self, name=None, initial=0.0, description=None, type=None, category=None):
        if not name:
            raise ValueError("Account name is required")
        if name in self._accounts:
            raise ValueError(f"Account '{name}' already exists")
        acct = Account(name=name, initial=initial, description=description, type=type, category=None)
        self._accounts[name] = acct
        self._add_as_property(acct)
        return acct

    def append(self, period_cashflows):
        cfs = list(period_cashflows)
        if cfs:
            self._journals._append_cashflows(cfs)
        return bool(cfs)

    @property
    def current_balances(self):
        cf = self._journals.postings
        return cf.groupby('acct')['amount'].sum()

    @property
    def balances_by_date(self):
        # kp: todo: what should this be called?
        bals = self._journals.postings[['date', 'acct', 'amount']].groupby(['date', 'acct'])
        return bals.sum().unstack().infer_objects().fillna(0).cumsum()['amount']

    @property
    def journals(self):
        return self._journals.journals.set_index('txn_id').sort_index()

    @property
    def postings(self):
        return self._journals.postings.sort_values('txn_id').set_index('txn_id')

    def sum(self, accts):
        accts = [a.name if isinstance(a, Account) else a for a in accts]
        bals = self.current_balances
        return self.current_balances.loc[accts].sum()

    @property
    def accounts(self):
        return self._accounts.values()

    def _assert_accounts_are_valid(self, cf):
        if isinstance(cf.from_acct, Account):
            cf = cf._replace(from_acct=cf.from_acct.name)
        if isinstance(cf.to_acct, Account):
            cf = cf._replace(to_acct=cf.to_acct.name)
        if cf.from_acct not in self._accounts:
            msg = f'Cashflow from account "{cf.from_acct}" is not a registered account: {self._accounts}'
            raise InvalidAccount(msg)
        if cf.to_acct not in self._accounts:
            msg = f'Cashflow to account "{cf.to_acct}" is not a registered account: {self._accounts}'
            raise InvalidAccount(msg)
        return cf

    def _add_as_property(self, acct):
        if hasattr(self, acct.name):
            raise ValueError(f"Account name '{acct.name}' is already in use")
        # kp: todo: property getter doesn't  work? get property back that is not evaluated for instances?
        #def getter(self):
        #    return self._accounts[acct.name]
        #setattr(Account, acct.name, property(getter))
        setattr(self, acct.name, acct)


class JournalEntries():

    def __init__(self, initial_cfs):
        self.journals = _journals(initial_cfs or [])

    def _append_cashflows(self, period_cashflows):
        period_journals = _journals(period_cashflows)
        self.journals = pd.concat([self.journals, period_journals])

    @property
    def postings(self):
        from_acct = self.journals[['txn_id', 'date', 'from_acct', 'amount', 'description', 'generator']].copy()
        from_acct['amount'] = from_acct['amount'] * -1
        from_acct.columns = ['txn_id', 'date', 'acct', 'amount', 'description', 'generator']
        to_acct = self.journals[['txn_id', 'date', 'to_acct', 'amount', 'description', 'generator']].copy()
        to_acct.columns = ['txn_id', 'date', 'acct', 'amount', 'description', 'generator']
        return pd.concat([from_acct, to_acct]).sort_values(['txn_id', 'acct'])


def _journals(cfs):
    df = pd.DataFrame(cfs, columns=('txn_id', 'date', 'amount', 'from_acct', 'to_acct', 'description', 'generator'))
    df['date'] = pd.to_datetime(df['date'])
    return df


class SimulationLoggerAdapter(logging.LoggerAdapter):

    def __init__(self, simulation, logger):
        self.simulation = simulation
        self.logger = logger

    def process(self, msg, kwargs):
        return (f"[{self.simulation.current_period}] {msg}", kwargs)

    def trace(self, msg, *args, **kwargs):
        self.log(5, msg, *args, **kwargs)


class StopSimulation(Exception):
    pass


class FailedToAwaitClock(Exception):
    pass


class FailedToYieldCashFlow(Exception):
    pass


class GeneratorExhausted(Exception):
    pass


class NotReadyAfterAll(Exception):
    pass


class InvalidWaitTime(Exception):
    pass


class InvalidCashFlowYielded(Exception):
    pass


class InvalidAccount(Exception):
    pass


class InvalidGenerator(Exception):
    pass
