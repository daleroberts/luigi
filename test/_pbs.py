import unittest
import luigi
import luigi.contrib.pbs as pbs


class A(luigi.Task):

    def run(self):
        self.has_run = True

    def complete(self):
        return self.has_run


class B(luigi.Task):

    def requires(self):
        return [A()]

    def run(self):
        self.has_run = True

    def complete(self):
        return self.has_run


class C(luigi.Task):

    def run(self):
        self.has_run = True

    def complete(self):
        return self.has_run


class TestTaskSerialise(unittest.TestCase):

    def setUp(self):
        pass

    def test_targets(self):
        self.assertEqual(sorted(pbs.targets([A()])), ['A()'])
        self.assertEqual(sorted(pbs.targets([B()])), ['A()', 'B()'])
        self.assertEqual(sorted(pbs.targets([C()])), ['C()'])
        self.assertEqual(sorted(pbs.targets([B(), C()])),
                         ['A()', 'B()', 'C()'])
        self.assertEqual(sorted(pbs.targets([A(), B(), C()])),
                         ['A()', 'B()', 'C()'])


if __name__ == '__main__':
    unittest.main()
