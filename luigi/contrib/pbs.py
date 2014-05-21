"""
PBS
"""
import cPickle as pickle
import os


def _dump(self, dir=''):
    """
    Dump instance to file.
    """
    file_name = os.path.join(dir, 'job-instance.pickle')
    if self.__module__ == '__main__':
        d = pickle.dumps(self)
        module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
        d = d.replace('(c__main__', "(c" + module_name)
        open(file_name, "w").write(d)
    else:
        pickle.dump(self, open(file_name, "w"))


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


def run(tasks, ncpus=1):
    """
    Run a list of tasks.
    """
    pass
