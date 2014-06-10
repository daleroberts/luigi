import unittest
import luigi
import luigi.contrib.pbs as pbs
from os.path import join as pjoin

TEMPDIR = pjoin('/tmp', 'work')


class DummyTask(luigi.Task):
    i = luigi.Parameter()

    def run(self):
        f = self.output().open('w')
        f.close()

    def output(self):
        return luigi.LocalTarget(pjoin(TEMPDIR, str(self.i)))


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


class TestPbs(unittest.TestCase):

    def test_targets(self):
        self.assertEqual(sorted(pbs.targets([A()])), ['A()'])
        self.assertEqual(sorted(pbs.targets([B()])), ['A()', 'B()'])
        self.assertEqual(sorted(pbs.targets([C()])), ['C()'])
        self.assertEqual(sorted(pbs.targets([B(), C()])),
                         ['A()', 'B()', 'C()'])
        self.assertEqual(sorted(pbs.targets([A(), B(), C()])),
                         ['A()', 'B()', 'C()'])

    def test_run(self):
        tasks = [DummyTask(i) for i in range(5)]
        # make sure outputs dont exist
        for task in tasks:
            if task.complete():
                task.output().remove()
        # run the tasks
        pbs.run(tasks, ncpus=1)
        # check for completion
        for t in tasks:
            print t.task_id, t.output().path, t.output().exists()
            #self.assertTrue(t.complete() is True)


if __name__ == '__main__':
    unittest.main()
