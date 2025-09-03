import pandas as pd
import unittest
from expecter import expect
from datetime import date
from cfs.simulation import Simulation, Accounts, AcctType
from cfs.cashflows import box_3_tax, amortizing_loan


class WhenGeneratingCashflows():

    def should_calc_box_3_as_30_percent_of_4_percent(self):
        box_3 = box_3_tax(('cash', 'investments'), 'cash', 'tax')
        accts = dict(
            cash=100000,
            investments=50000,
            tax=0,
        )
        sim = Simulation(accts=accts, start_date=date(2019, 1, 1), end_date=date(2020, 1, 1)).add(*box_3).run()
        expect(sim.accts.current_balances['tax']) == 150000 * (1.2 / 100)
        print("=== JOURNALS ===")
        print(sim.accts.journals)
        print("=== POSTINGS ===")
        print(sim.accts.postings)
        print("=== BALANCES THRU TIME ===")
        print(sim.accts.balances_by_date)
        print("=== FINAL BALANCES ===")
        print(sim.accts.current_balances)

    def should_also_be_able_to_use_richer_acct_data(self):
        accts = Accounts()
        cash = accts.add(name='cash', initial=100000, type=AcctType.ASSET)
        investments = accts.add(name='investments', initial=50000, type=AcctType.ASSET)
        tax = accts.add(name='tax', initial=0, type=AcctType.EXPENSE)

        box_3 = box_3_tax((cash, investments), cash, tax)
        sim = Simulation(accts=accts, start_date=date(2019, 1, 1), end_date=date(2020, 1, 1)).add(*box_3).run()
        expect(sim.accts.current_balances['tax']) == 150000 * (1.2 / 100)



    def should_calculate_amortizing_loans_correctly(self):
        accts = Accounts({'payments': 0, 'mortgage':0, 'principal': 0, 'interest': 0})
        mortgage = amortizing_loan(
            principal=300_000,
            rate=0.05,
            years=1,
            payment_acct=accts.payments,
            loan_acct=accts.mortgage,
            principal_acct=accts.principal,
            interest_acct=accts.interest,
        )
        sim = Simulation(accts=accts, start_date=date(2020, 1, 1)).add(*mortgage).run()
        print(sim.accts.balances_by_date)
        expect(sim.accts.balances_by_date.loc[pd.to_datetime(date(2020, 1, 1)), 'principal']) == -300_000
        expect(round(sim.accts.balances_by_date.loc[pd.to_datetime(date(2020, 1, 31)), 'interest'], 2)) == 1250.0
        expect(round(sim.accts.balances_by_date.loc[pd.to_datetime(date(2020, 1, 31)), 'principal'], 1)) == -275567.8
        expect(round(sim.accts.current_balances['principal'], 2)) == 0.0
        expect(len(sim.accts.balances_by_date.index)) == 1 + 12 # drawdown followed by 12 monthly installments
        expect(round(sim.accts.sum(['payments', 'interest', 'mortgage']), 2)) == 0.0
        expect(round(sim.accts.sum(['interest']), 2)) == 8186.93
