import numpy as np
import numpy_financial as npf

import logging
logger = logging.getLogger('cashflows')


def amortizing_loan(principal=None, rate=None, years=None,
                    payment_acct=None, loan_acct=None,
                    principal_acct=None, interest_acct=None):
    assert principal
    assert rate
    assert years
    assert payment_acct
    assert principal_acct
    assert interest_acct
    if not loan_acct:
        loan_acct = principal_acct

    # kp: todo: this leads to different total interest than using years, given amort happens over whole year
    monthly_rate = rate / 12
    months = years * 12
    periods = np.arange(months) + 1
    amort_schedule = npf.ppmt(monthly_rate, periods, months, principal) * -1
    int_schedule = npf.ipmt(monthly_rate, periods, months, principal) * -1

    async def amortizing_loan_cfs(sim):
        yield sim.cf(principal, principal_acct, loan_acct, 'Initial loan draw')
        await sim.clock.tick(months=1, days=-1)
        for period in range(months):
            if sim.accts.sum([principal_acct.name]) >= 0:
                sim.logger.info('Loan fully paid off: stopping payments')
                return
            amortization_payment = amort_schedule[period]
            yield sim.cf(amortization_payment, payment_acct, principal_acct, f'Amortization payment for period {period+1}/ {months}')

            interest_payment = int_schedule[period]
            yield sim.cf(interest_payment, payment_acct, interest_acct, f'Interest payment for period {period+1}/ {months}')

            await sim.clock.tick(months=1)
        await sim.clock.tick(days=1)
        yield sim.cf(balances[principal_acct] * -1, payment_acct, principal_acct, 'Paydown')

    yield amortizing_loan_cfs


def interest_only_loan(principal=None, rate=None, 
                       payment_acct=None, loan_acct=None,
                       principal_acct=None, interest_acct=None):
    assert principal
    assert rate
    assert payment_acct
    assert principal_acct
    assert interest_acct
    if not loan_acct:
        loan_acct = principal_acct

    monthly_rate = rate / 12.

    async def interest_only_loan_payment(sim):
        yield sim.cf(principal, principal_acct, loan_acct, 'Initial loan draw')
        await sim.clock.tick(months=1, days=-1)
        while True:
            bal = sim.accts.sum([principal_acct]) * -1
            if bal <= 0:
                sim.logger.info(f"Loan paid off, stopping interest payments")
                return
            interest = monthly_rate * bal
            yield sim.cf(interest, src=payment_acct, dst=interest_acct, desc=f'Monthly interest-only payment: {monthly_rate:.2%} x {bal}')
            await sim.clock.tick(months=1)
    yield interest_only_loan_payment


def fire_income(annual_rate=4.0, personal_acct=None, investment_accts=None, blackhole_acct=None):
    assert personal_acct
    assert investment_accts
    assert blackhole_acct

    async def fire_investment_income(sim):
        # figure this out at start I think and keeps it constant?
        capital = sim.accts.sum(investment_accts)
        assert capital
        annual_withdrawal = annual_rate / 100. * capital
        monthly_withdrawal = annual_withdrawal / 12.

        sim.logger.info(f"Capital: {capital}; annual %: {annual_rate}; annual withdrawal: {annual_withdrawal}; monthly: {monthly_withdrawal}")
        await sim.clock.until_day(15)
        while True:
            yield sim.cf(monthly_withdrawal, src=blackhole_acct, dst=personal_acct, desc='Monthly FIRE withdrawal')
            await sim.clock.tick(months=1)
    yield fire_investment_income


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
                    yield sim.cf(high_tax, income_acct, tax_acct, f'{high_rate:.1%} Corp Income Tax on {higher_rate_amount:.2f}')
                lower_rate_amount = min(annual_income, threshold)
                low_tax = lower_rate_amount * low_rate
                retained_earnings += lower_rate_amount - low_tax
                yield sim.cf(low_tax, income_acct, tax_acct, f'{low_rate:.1%} Corp Income Tax on {lower_rate_amount:.2f}')
                yield sim.cf(retained_earnings, income_acct, retained_earnings_acct, 'BV Retained Earnings')
                assert retained_earnings + low_tax + high_tax == annual_income
    yield bv_corp_tax


def bv_dividend_payment(retained_earnings_acct, tax_acct, personal_acct, max_dividend=None):
    """ Pay all (or up to `max_dividend`) of retained earnings out as dividend / tax """
    threshold = 67804*2.
    low_rate = 0.245
    high_rate = 0.31
    async def dividend_payments(sim):
        while True:
            await sim.clock.next_calendar_year_end()
            await sim.clock.tick(days=1)  # ensure after corp tax
            dividend = sim.accts.sum([retained_earnings_acct])
            if dividend > 0:
                if max_dividend:
                    dividend = min(dividend, max_dividend)
                high_tax = 0
                if dividend > threshold:
                    higher_rate_amount = dividend - threshold
                    higher_tax = higher_rate_amount * high_rate
                    yield sim.cf(higher_tax, retained_earnings_acct, tax_acct, f'{high_rate:.1%} Box 2 Dividend Tax on {higher_rate_amount:.2f}')
                lower_rate_amount = min(dividend, threshold)
                low_tax = lower_rate_amount * low_rate
                yield sim.cf(low_tax, retained_earnings_acct, tax_acct, f'{low_rate:.1%} Box 2 Dividend Tax on {lower_rate_amount:.2f}')
                yield sim.cf(dividend - low_tax - high_tax, retained_earnings_acct, personal_acct, 'Dividend payment')
    yield dividend_payments


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
