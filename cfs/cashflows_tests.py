import unittest
from expecter import expect
from datetime import date
from cfs.simulation import Simulation, Account, AcctType
from cfs.cashflows import box_3_tax 


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
        cash = Account(name='cash', initial=100000, type=AcctType.ASSET)
        investments = Account(name='investments', initial=50000, type=AcctType.ASSET)
        tax = Account(name='tax', initial=0, type=AcctType.EXPENSE)

        box_3 = box_3_tax((cash, investments), cash, tax)
        sim = Simulation(accts=[cash, investments, tax], start_date=date(2019, 1, 1), end_date=date(2020, 1, 1)).add(*box_3).run()
        expect(sim.accts.current_balances['tax']) == 150000 * (1.2 / 100)
