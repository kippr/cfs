import numpy as np

import logging
logger = logging.getLogger('cashflows')


def amortizing_loan(principal=None, rate=None, years=None, amort_years=None,
                    payment_acct=None, principal_acct=None, interest_acct=None):
    periods = np.arange(amort_years) + 1
    amort_schedule = np.ppmt(rate, periods, amort_years, principal) * -1
    int_schedule = np.ipmt(rate, periods, amort_years, principal) * -1

    async def amortizing_loan_principal_cfs(clock, balances):
        yield sim.cf(principal, principal_acct, payment_acct, 'Initial loan draw')
        await clock.tick(years=1, days=-1)
        for period in range(years - 1):
            amortization_payment = amort_schedule[period]
            yield amortization_payment, payment_acct, principal_acct, 'Amortization payment'
            await clock.tick(years=1)
        amortization_payment = amort_schedule[period + 1]
        yield sim.cf(amortization_payment, payment_acct, principal_acct, 'Amortization payment')
        await clock.tick(days=1)
        yield sim.cf(balances[principal_acct] * -1, payment_acct, principal_acct, 'Paydown')

    async def amortizing_loan_interest_cfs(clock, balances):
        await clock.tick(years=1, days=-1)
        for period in range(years):
            interest_payment = int_schedule[period]
            yield sim.cf(interest_payment, payment_acct, interest_acct, 'Interest payment')
            await clock.tick(years=1)
    yield amortizing_loan_principal_cfs
    yield amortizing_loan_interest_cfs


def bv_corp_tax(income_acct, tax_acct, retained_earnings_acct):
    """ Generate cashflows that take all BV income from income_acct for a year and calculate tax_acct  vs
    retained_earnings_acct split """
    threshold = 200_000.
    low_rate = 0.19
    high_rate = 0.258
    async def bv_corp_tax(sim):
        while True:
            await sim.clock.next_calendar_year_end()
            annual_income = sim.accts.sum([income_acct])
            retained_earnings = 0
            high_tax = 0
            if annual_income:
                if annual_income > threshold:
                    higher_rate_amount = annual_income - threshold
                    high_tax = higher_rate_amount * high_rate
                    retained_earnings = higher_rate_amount - high_tax
                    yield sim.cf(high_tax, income_acct, tax_acct, f'{high_rate:.1%} Corp Income Tax on {higher_rate_amount}')
                lower_rate_amount = min(annual_income, threshold)
                low_tax = lower_rate_amount * low_rate
                retained_earnings += lower_rate_amount - low_tax
                yield sim.cf(low_tax, income_acct, tax_acct, f'{low_rate:.1%} Corp Income Tax on {lower_rate_amount}')
                yield sim.cf(retained_earnings, income_acct, retained_earnings_acct, 'BV Retained Earnings')
                assert retained_earnings + low_tax + high_tax == annual_income
    yield bv_corp_tax


def bv_pay_retained_earnings_as_dividend(retained_earnings_acct, tax_acct, personal_acct):
    """ Pay all retained earnings out as dividend / tax """
    async def dividend_sweep(clock, balances):
        while True:
            await clock.next_calendar_year_end()
            dividend = balances[retained_earnings_acct]
            if dividend > 0:
                yield dividend * 0.25 , retained_earnings_acct, tax_acct, 'Dividend payment: tax'
                yield dividend * 0.75, retained_earnings_acct, personal_acct, 'Dividend payment'
    yield dividend_sweep


def box_3_tax(net_worth_accts_or_filter, pers_cash_act, pers_tax_acct):
    async def box_3_tax(sim):
        while True:
            await sim.clock.next_calendar_year_end()
            if callable(net_worth_accts_or_filter):
                net_worth_accts = filter(net_worth_accts_or_filter, sim.accts.accounts)
                logger.debug(list(net_worth_accts))
            else:
                net_worth_accts = net_worth_accts_or_filter
            net_worth = sim.accts.sum(net_worth_accts)
            if net_worth and net_worth > 0:
                yield sim.cf(net_worth * 0.3 * 0.04, pers_cash_act, pers_tax_acct, f'Box 3 payment on net worth of {net_worth:.2f}')
    yield box_3_tax
