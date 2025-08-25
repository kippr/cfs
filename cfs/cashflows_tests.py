import unittest
from expecter import expect
from datetime import date
from cfs.simulation import Simulation
from cfs.cashflows import box_3_tax 


class WhenGeneratingCashflows():

    def should_calc_box_3_as_30_percent_of_4_percent(self):
        box_3 = box_3_tax(('cash', 'investments'), 'cash', 'tax')
        accts = dict(
            cash=100000,
            investments=50000,
        )
        sim = Simulation(accts=accts, start_date=date(2019, 1, 1), end_date=date(2020, 1, 1)).add(*box_3).run()
        expect(sim.accts['tax']) == 150000 * (1.2 / 100)
        print("=== JOURNALS ===")
        print(sim.accts.journals)
        print("=== POSTINGS ===")
        print(sim.accts.postings)
        print("=== BALANCES THRU TIME ===")
        print(sim.accts.balances_by_date)
        print("=== FINAL BALANCES ===")
        print(sim.accts.current_balances)
