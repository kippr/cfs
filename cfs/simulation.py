from collections import namedtuple
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import time
import types
import functools
import logging
import asyncio
logging.addLevelName(5, 'TRACE')
logging.TRACE = 5


class Simulation(object):
    """ Cash flow generators generate cash flows.. duh. But sometimes they want to wait for clock to progress. Might
    be nice to look at asyncio to use coroutines to interleave these things rather than magic yield statements
    (and needing to use yield from)?
    """
    cashflows = None
    id_fountain = None
    last_period = None

    def __init__(self, *cashflow_generators, start_date=None, end_date=None):
        assert start_date < end_date, 'Start date must be < end_date'
        self.logger = self._logger(self.__class__, 'Main')
        self.start_date, self.current_period, self.end_date = start_date, start_date, end_date
        # kp: todo: kill cashflow_generators from param list?
        self.generators = tuple(cashflow_generators)

    def add(self, *cashflow_generators):
        self.generators = self.generators + tuple(cashflow_generators)

    def _prepare_run(self):
        if self.cashflows is not None:
            raise GeneratorExhausted("Simulation already run?")
        self.cashflows = cashflows_df([])
        self.id_fountain = iter(range(10000000000))
        self.last_period = False
        self.generators = tuple(self._setup_generators())
        self.logger.info("Initialized cashflow generators: %s",
                         [f'{g.generator.__name__}' for g in self.generators])

    def _setup_generators(self):
        for cashflow_generator in self.generators:
            clock = Clock(self, cashflow_generator)
            balances = Balances(self, generator_name=cashflow_generator.__name__)
            iter_cashflows = cashflow_generator(clock, balances)
            logger = self._logger(cashflow_generator, 'Cashflows')
            yield GeneratorAttributes(cashflow_generator, iter_cashflows, clock, balances, logger)

    def _logger(self, generator, label):
        logger = logging.getLogger(f'{generator.__name__}:{label}')
        return SimulationLoggerAdapter(self, logger)

    def _period_cashflows(self):
        for g in self.generators[:]:
            if g.clock.ready:
                try:
                    cf = _next(g.iter_cashflows)
                except GeneratorExhausted:
                    g.logger.info("Exhausted.. removing.")
                    self.generators = tuple(x for x in self.generators if g != x)
                except NotReadyAfterAll:
                    g.logger.debug("No cfs yet after all.. Two awaits in a row.")
                else:
                    if cf == WAITING:
                        g.clock._wait_was_awaited()
                        g.logger.trace('will wait until %s', g.clock.waiting_for)
                    elif isinstance(cf, (list, tuple)) and len(cf) == 4:
                        g.clock._cf_was_yielded()
                        amount, from_acct, to_acct, description = cf
                        g.logger.info('Transfer %s from %s to %s: "%s"', cf[0], cf[1], cf[2], cf[3])
                        txn_id = next(self.id_fountain)
                        yield txn_id, self.current_period, from_acct, to_acct, amount, description
                    else:
                        msg = f'Expected a cashflow (amount, from, to, description) but got "{cf}"'
                        g.logger.error(msg)
                        raise InvalidCashFlowYielded(msg)

    def _advance_period(self, must_advance):
        if not self.generators:
            raise StopSimulation('All cashflow generators exhausted. Stopping simulation')
        new_period = min((g.clock.waiting_for for g in self.generators if g.clock.waiting_for))
        if must_advance and new_period == self.current_period:
            raise StopSimulation( "No cashflows this period and no generator waiting for future period.. stopping early")
        if self.current_period == new_period:
            self.logger.trace("Staying on %s to check for more cashflows", self.current_period)
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
                self.logger.info("Advancing to %s", self.current_period)

    @property
    def current_balances(self):
        cf = self._exploded_cashflows
        if self.current_period:
            cf = cf.loc[cf['date'] < self.current_period]
        return cf.groupby('acct')['amount'].sum()

    @property
    def balances(self):
        bals = self._exploded_cashflows[['date', 'acct', 'amount']].groupby(['date', 'acct'])
        return bals.sum().unstack().fillna(0).cumsum()['amount']

    def run(self):
        try:
            self._prepare_run()
            while True:
                period_cashflows = cashflows_df(self._period_cashflows())
                must_advance = len(period_cashflows) == 0
                self._append_cashflows(period_cashflows)
                try:
                    self._advance_period(must_advance)
                except StopSimulation as e:
                    self.logger.info(str(e))
                    self.current_period = None
                    break
        finally:
            cleanup_simulation_loop()
        return self

    def _append_cashflows(self, period_cashflows):
        self.cashflows = pd.concat([self.cashflows, period_cashflows])

    @property
    def _exploded_cashflows(self):
        from_cf = self.cashflows[['date', 'from_acct', 'amount']].copy()
        from_cf['amount'] = from_cf['amount'] * -1
        from_cf.columns = ['date', 'acct', 'amount']
        to_cf = self.cashflows[['date', 'to_acct', 'amount']]
        to_cf.columns = ['date', 'acct', 'amount']
        return pd.concat([from_cf, to_cf])

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


class AsyncGeneratorWrapper:
    """Wrapper to handle async generators in Python 3.12"""
    def __init__(self, async_gen):
        self._gen = async_gen
        self._coro = None
        
    def __anext__(self):
        if self._coro is None:
            self._coro = self._gen.__anext__()
        return self._coro
        
    async def get_next(self):
        try:
            result = await self._gen.__anext__()
            self._coro = None  # Reset for next call
            return result
        except StopAsyncIteration:
            raise GeneratorExhausted()

# Global event loop for the simulation
_simulation_loop = None

def get_simulation_loop():
    global _simulation_loop
    if _simulation_loop is None or _simulation_loop.is_closed():
        _simulation_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_simulation_loop)
    return _simulation_loop

def cleanup_simulation_loop():
    global _simulation_loop
    if _simulation_loop and not _simulation_loop.is_closed():
        _simulation_loop.close()
    _simulation_loop = None

def _next(iter_cashflows):
    # grab the next cash flow from the async generator or else a clock waiting event
    try:
        loop = get_simulation_loop()
        if not loop.is_running():
            # Run the async function in the persistent loop
            coro = _async_next(iter_cashflows)
            return loop.run_until_complete(coro)
        else:
            # If loop is already running, we need to create a task
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, _async_next(iter_cashflows))
                return future.result()
    except AttributeError as e:
        raise NotReadyAfterAll()

async def _async_next(iter_cashflows):
    """Async helper function to get next value from async generator"""
    try:
        result = await iter_cashflows.__anext__()
        return result
    except StopAsyncIteration:
        raise GeneratorExhausted()


class AsyncGeneratorSuspend(Exception):
    """Exception used to suspend async generator execution"""
    def __init__(self, waiting_result):
        self.waiting_result = waiting_result
        super().__init__()

class AsyncGeneratorYield:
    """A special awaitable that yields a value from an async generator"""
    def __init__(self, value):
        self.value = value
    
    def __await__(self):
        # This is what gets awaited in the async generator
        async def _await_impl():
            # This coroutine completion signals that we should yield the value
            return self.value
        return _await_impl().__await__()

class AwaitableWaiting:
    """A special object that can be both awaited and used with next() for backward compatibility"""
    
    def __init__(self, clock=None):
        self.clock = clock
    
    def __await__(self):
        # Clear the await flag since this was properly awaited
        if self.clock:
            self.clock._awaiting_clock_wait = False
        # Return an awaitable that yields WAITING when awaited
        return AsyncGeneratorYield(WAITING).__await__()
        
    def __iter__(self):
        # For next() usage in tests
        return self
        
    def __next__(self):
        # For next() usage in tests
        # Clear the await flag since this is being used in test context
        if self.clock:
            self.clock._awaiting_clock_wait = False
        return WAITING
        
def enforce_awaited(wrapped):
    @functools.wraps(wrapped)
    def checked(clock, *args, **kwargs):
        clock._awaiting_clock_wait = wrapped.__name__
        result = wrapped(clock, *args, **kwargs)
        # The result will be an AwaitableWaiting, so we don't reset the flag here
        # It will be reset when the actual waiting happens
        return result
    return checked


class Clock(object):

    def __init__(self, simulation, generator):
        self.simulation = simulation
        self._waiting_for = simulation.start_date
        self.logger = simulation._logger(generator, 'Clock')
        self._awaiting_clock_wait = False

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
        if waiting_for < self.current_period:
            msg = f'Requesting wait until {waiting_for}, but that has already passed; currently at {self.current_period}'
            raise InvalidWaitTime(msg)
        self._waiting_for = waiting_for

    @property
    def elapsed(self):
        return relativedelta(self.current_period, self.simulation.start_date)

    @enforce_awaited
    def tick(self, **kwargs):
        date = self.current_period + relativedelta(**kwargs)
        return self.until(date)

    @enforce_awaited
    def until(self, date):
        self.waiting_for = date
        # Return a special object that can be both awaited and used with next()
        return AwaitableWaiting(self)

    @enforce_awaited  
    def next_calendar_year_end(self):
        """Wait until next 31 Dec"""
        self.waiting_for = datetime.date(self.current_period.year, 12, 31)
        if self.waiting_for == self.current_period:
            self.waiting_for = datetime.date(self.current_period.year + 1, 12, 31)
        return AwaitableWaiting(self)

    def _wait_was_awaited(self):
        self._awaiting_clock_wait = False

    def _cf_was_yielded(self):
        if self._awaiting_clock_wait:
            func_name = self._awaiting_clock_wait
            msg = f"You called clock.{func_name} without awaiting the result (use 'await clock.{func_name}(..)' instead"
            self.logger.error(msg)
            raise FailedToAwaitClock(msg)



def assert_accounts(*registered_accounts):
    """ This decorator can be added to your cash flow generators to assert they only access accounts specifically
    registered. This can help prevent bugs.
    """
    def decorator(func):
        @functools.wraps(func)
        async def assert_account_access(clock, balances, **kargs):
            strict_balances = StrictBalances(registered_accounts, balances)
            async for cf in func(clock, strict_balances):
                # kp: todo: don't like this here and in main loop.. better way? yield something meaningful?
                if isinstance(cf, (list, tuple)) and len(cf) == 4:
                    amount, from_acct, to_acct, description = cf
                    strict_balances._assert_accts_known(from_acct, to_acct)
                yield cf
        return assert_account_access
    return decorator


class StrictBalances(object):

    def __init__(self, registered_accounts, balances):
        self.registered_accounts = registered_accounts
        self.balances = balances

    def __getitem__(self, acct):
        self._assert_accts_known(acct)
        return self.balances[acct]

    def sum(self, accts):
        self._assert_accts_known(*accts)
        return self.balances.sum(accts)

    def _assert_accts_known(self, *accounts_to_check):
        for acct in accounts_to_check:
            if acct not in self.registered_accounts:
                raise InvalidAccount(f"'{acct}' not pre-registered with generator '{self.generator_name}'. Registered: '{self.registered_accounts}'")

    @property
    def generator_name(self):
        return self.balances.generator_name

    @property
    def accounts(self):
        return self.balances.accounts


class Balances(object):

    def __init__(self, simulation, generator_name='unnamed'):
        self.simulation = simulation
        self.generator_name = generator_name

    def __getitem__(self, acct):
        try:
            return self.simulation.current_balances[acct]
        except KeyError:
            return 0

    def sum(self, accts):
        try:
            return self.simulation.current_balances.loc[list(accts)].sum()
        except KeyError:
            return 0

    @property
    def accounts(self):
        return self.simulation.current_balances.index.tolist()


def cashflows_df(cfs):
    return pd.DataFrame(cfs, columns=('txn_id', 'date', 'from_acct', 'to_acct', 'amount', 'description'))


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


GeneratorAttributes = namedtuple('GeneratorAttributes', ('generator', 'iter_cashflows', 'clock', 'balances', 'logger'))


WAITING = object()
