import numpy as np

from cfs.simulation import assert_accounts

import logging
logger = logging.getLogger('cashflows')


INITIAL_BALANCE_DESCRIPTION = 'Initial balance'
def initial(initial_cfs):
    """ Yield 'starting balances' from passed initial_cfs, which should take form of:
        ((amount, from_acct, to_acct), ..)"""
    async def initial_cashflows(clock, balances):
        for amount, from_acct, to_acct in initial_cfs:
            yield amount, from_acct, to_acct, INITIAL_BALANCE_DESCRIPTION
    yield initial_cashflows


def amortizing_loan(principal=None, rate=None, years=None, amort_years=None,
                    payment_acct=None, principal_acct=None, interest_acct=None):
    periods = np.arange(amort_years) + 1
    amort_schedule = np.ppmt(rate, periods, amort_years, principal) * -1
    int_schedule = np.ipmt(rate, periods, amort_years, principal) * -1

    @assert_accounts(payment_acct, principal_acct)
    async def amortizing_loan_principal_cfs(clock, balances):
        yield principal, principal_acct, payment_acct, 'Initial loan'
        await clock.tick(years=1, days=-1)
        for period in range(years - 1):
            amortization_payment = amort_schedule[period]
            yield amortization_payment, payment_acct, principal_acct, 'Amortization payment'
            await clock.tick(years=1)
        amortization_payment = amort_schedule[period + 1]
        yield amortization_payment, payment_acct, principal_acct, 'Amortization payment'
        await clock.tick(days=1)
        yield balances[principal_acct] * -1, payment_acct, principal_acct, 'Paydown'

    @assert_accounts(payment_acct, interest_acct)
    async def amortizing_loan_interest_cfs(clock, balances):
        await clock.tick(years=1, days=-1)
        for period in range(years):
            interest_payment = int_schedule[period]
            yield interest_payment, payment_acct, interest_acct, 'Interest payment'
            await clock.tick(years=1)
    yield amortizing_loan_principal_cfs
    yield amortizing_loan_interest_cfs


def bv_corp_tax(income_acct, tax_acct, retained_earnings_acct):
    """ Generate cashflows that take all BV income from income_acct for a year and calculate tax_acct  vs
    retained_earnings_acct split """
    async def bv_corp_tax(clock, balances):
        while True:
            await clock.next_calendar_year_end()
            annual_income = balances[income_acct]
            if annual_income:
                if annual_income > 200000:
                    higher_rate_amount = annual_income - 200000
                    yield higher_rate_amount * 0.25, income_acct, tax_acct, '25% Corp Income Tax'
                    yield higher_rate_amount * 0.75, income_acct, retained_earnings_acct, 'BV Retained Earnings (after 25% corp tax)'
                lower_rate_amount = min(annual_income, 200000)
                yield lower_rate_amount * 0.2, income_acct, tax_acct, '20% Corp Income Tax'
                yield lower_rate_amount * 0.8, income_acct, retained_earnings_acct, 'BV Retained Earnings (after 20% corp tax)'
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
    async def box_3_tax(clock, balances):
        while True:
            await clock.next_calendar_year_end()
            if callable(net_worth_accts_or_filter):
                net_worth_accts = filter(net_worth_accts_or_filter, balances.accounts)
                logger.debug(list(net_worth_accts))
            else:
                net_worth_accts = net_worth_accts_or_filter
            net_worth = balances.sum(net_worth_accts)
            if net_worth and net_worth > 0:
                yield net_worth * 0.3 * 0.04, pers_cash_act, pers_tax_acct, f'Box 3 payment on net worth of {net_worth:.2f}'
    yield box_3_tax
