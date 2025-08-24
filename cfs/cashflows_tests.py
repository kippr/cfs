import unittest
from expecter import expect
from datetime import date
from cfs.simulation import Simulation
from cfs.cashflows import box_3_tax, initial, INITIAL_BALANCE_DESCRIPTION


class WhenGeneratingCashflows():

    def should_calc_box_3_as_30_percent_of_4_percent(self):
        # kp: todo: replace initial generator with Accounts with initial balances?
        initial_cfs = initial([(100000, 'starting', 'cash'), (50000, 'starting', 'investments')])
        box_3 = box_3_tax(('cash', 'investments'), 'cash', 'tax')
        sim = Simulation(initial_cfs, box_3, start_date=date(2019, 1, 1), end_date=date(2020, 1, 1)).run()
        expect(sim.accounts['tax']) == 150000 * (1.2 / 100)
        print("=== JOURNALS ===")
        print(sim.accounts.journals)
        print("=== POSTINGS ===")
        print(sim.accounts.postings)
        print("=== BALANCES THRU TIME ===")
        print(sim.accounts.balances_by_date)
        print("=== FINAL BALANCES ===")
        print(sim.accounts.current_balances)
