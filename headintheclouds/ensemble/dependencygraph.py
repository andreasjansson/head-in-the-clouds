import collections

class DependencyGraph(object):

    def __init__(self):
        self.graph = collections.defaultdict(set)
        self.inverse_graph = collections.defaultdict(set)
        self.dependent_pointers = collections.defaultdict(
            lambda: collections.defaultdict(set))

    def add(self, dependent, pointer, depends):
        self.graph[depends].add(dependent)
        self.inverse_graph[dependent].add(depends)
        self.dependent_pointers[depends][dependent].add(pointer)

    def remove(self, dependent, pointer, depends):
        self.dependent_pointers[depends][dependent] = self.dependent_pointers[depends][dependent] - {pointer}
        if not self.dependent_pointers[depends][dependent]:
            del self.dependent_pointers[depends][dependent]

            self.graph[depends] = self.graph[depends] - {dependent}
            if not self.graph[depends]:
                del self.graph[depends]

            self.inverse_graph[dependent] = self.inverse_graph[dependent] - {depends}
            if not self.inverse_graph[dependent]:
                del self.inverse_graph[dependent]

            if not self.dependent_pointers[depends]:
                del self.dependent_pointers[depends]

    def get_depends(self, dependent):
        return self.inverse_graph[dependent]

    def get_dependents(self, depends):
        return self.dependent_pointers[depends]

    def find_cycle(self):
        nodes = set()
        for depends, dependent_list in self.graph.items():
            nodes.add(depends)
            nodes |= dependent_list

        graph = dict(self.graph)

        # guido's algorithm
        def dfs(node):
            if node in graph:
                for neighbour in graph[node]:
                    yield neighbour
                    dfs(neighbour)

        todo = set(nodes)
        while todo:
            node = todo.pop()
            stack = [node]
            while stack:
                top = stack[-1]
                for node in dfs(top):
                    if node in stack:
                        return stack[stack.index(node):]
                    if node in todo:
                        stack.append(node)
                        todo.remove(node)
                        break
                else:
                    node = stack.pop()

        return None

    def get_free_nodes(self, all_nodes):
        return all_nodes - set(self.inverse_graph)
