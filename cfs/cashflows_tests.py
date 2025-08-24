import unittest
from expecter import expect
from datetime import date
from cfs.simulation import Simulation
from cfs.cashflows import box_3_tax, initial, INITIAL_BALANCE_DESCRIPTION


class WhenGeneratingCashflows():

    def first_non_initial_cf(self, test_generator,
                             initial_cfs=((100000, 'starting', 'cash'), (50000, 'starting', 'investments'))):
        sim = Simulation(start_date=date(2019, 1, 1), end_date=date(2020, 1, 1))
        sim.add(*initial(initial_cfs))
        sim.add(*test_generator)
        sim.run()
        return next(x for x in sim.cashflows.itertuples() if x.description != INITIAL_BALANCE_DESCRIPTION)

    def should_calc_box_3_as_30_percent_of_4_percent(self):
        box_3 = box_3_tax(('cash', 'investments'), 'cash', 'tax')
        cf = self.first_non_initial_cf(box_3)
        expect(cf.amount) == 150000 * 1.2 / 100
