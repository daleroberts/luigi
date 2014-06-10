"""
PBS Job
-------

"""
import pickle
import signal
import logging
import subprocess
import sys
import os

import luigi.task

from os.path import abspath, join as pjoin, exists as pexists

TEMPLATE = """
python -c 'import luigi.contrib.pbs as pbs; pbs.main()
"""

TASKDIR = '/tmp/tasks'
TASKFILE = '%s.pickle'

logger = logging.getLogger('luigi-interface')


class PbsRunContext(object):

    def __init__(self):
        self.job_id = None

    def __enter__(self):
        self.__old_signal = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, self.kill_job)
        return self

    def kill_job(self, captured_signal=None, stack_frame=None):
        if self.job_id:
            logger.info('Job interrupted, killing job %s', self.job_id)
            subprocess.call(['mapred', 'job', '-kill', self.job_id])
        if captured_signal is not None:
            # adding 128 gives the exit code corresponding to a signal
            sys.exit(128 + captured_signal)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is KeyboardInterrupt:
            self.kill_job()
        signal.signal(signal.SIGTERM, self.__old_signal)


class Runner(object):

    def __init__(self, task=None, filename=None):
        self.task = task or pickle.load(open(filename))

    def run(self, stdin=sys.stdin, stdout=sys.stdout):
        dst = self.task.output().path
        if not os.path.exists(os.path.dirname(dst)):
            os.makedirs(os.path.dirname(dst))
        self.task.run()


def task_graph(tasks):
    """
    Generate a task graph.
    """
    nodes = set()
    edges = set()
    stack = list(tasks)
    while stack:
        current = stack.pop()
        nodes.add(current.task_id)
        for dep in current.deps():
            stack.append(dep)
            edges.add((current.task_id, dep.task_id))
    return (list(nodes), list(edges))


def targets(tasks):
    """
    Return all targets.
    """
    nodes, _ = task_graph(tasks)
    return nodes


def run(tasks, ncpus=1, filename=TASKFILE):
    """
    Run a list of tasks.
    """
    module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
    print 'module_name', module_name

    for task in tasks:
        filename = pjoin(TASKDIR, TASKFILE % task.task_id)
        if not os.path.exists(TASKDIR):
            os.makedirs(TASKDIR)
        with open(filename, 'w') as f:
            if task.__module__ == '__main__':
                d = pickle.dumps(task)
                d = d.replace('(c__main__', "(c" + module_name)
                f.write(d)
            else:
                pickle.dump(task, f)

    f = __file__
    if f.endswith('pyc'):
        f = f[:-3] + 'py'
    argv = ['python', abspath(f)]
    popen = subprocess.Popen(argv,
                             cwd=os.getcwd(),
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)

    # os.unlink(filename)


def print_exception(exc):
    import traceback
    tb = traceback.format_exc(exc)
    print >> sys.stderr, 'luigi-exc-hex=%s' % tb.encode('hex')


def main(stdin=sys.stdin, stdout=sys.stdout, print_exception=print_exception):
    try:
        logging.basicConfig(level=logging.WARN)
        taskfiles = [pjoin(TASKDIR,f) for f in os.listdir(TASKDIR) if os.path.isfile(pjoin(TASKDIR, f))]
        for taskfile in taskfiles:
            Runner(filename=taskfile).run(stdin=stdin, stdout=stdout)
    except Exception, exc:
        print_exception(exc)
        raise

if __name__ == '__main__':
    main()
