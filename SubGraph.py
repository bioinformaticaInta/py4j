#!/usr/bin/python

import py4j.DataType as DataType

class SubGraph:
    def __init__(self, nodes:set, rels:set):
        self.__nodes = nodes
        self.__relationships = rels

    def __repr__(self):
        return "Nodes: %s\n\nRelationships: %s" % (self.__nodes, self.__relationships)

    def separateNodes(self):
        nodes = {}
        for node in self.__nodes:
            nodeKey = (node.labels(),node.propNames())
            nodeValue = node.properties()
            if nodeKey in nodes:
                nodes[nodeKey].append(nodeValue)
            else:
                nodes[nodeKey] = [nodeValue]
        return nodes

    def separateRelationships(self):
        rels = {}
        for rel in self.__relationships:
            relKey = ((rel.startNode().labels(),rel.startNode().propNames()),
                      ((rel.type(),), rel.propNames()),
                      (rel.endNode().labels(),rel.endNode().propNames())
                     )
            relValue = rel.startNode().propertiesForUnwind("start") | \
                       rel.propertiesForUnwind("rel") | \
                       rel.endNode().propertiesForUnwind("end")
            if relKey in rels:
                rels[relKey].append(relValue)
            else:
                rels[relKey] = [relValue]
        return rels

    def addNode(self, node):
        self.__nodes.add(node)
    
    def addRelationship(self, rel):
        self.__relationships.add(rel)

    def nodes(self):
        return self.__nodes

    def relationships(self):
        return self.__relationships
    
    def __or__(sub1, sub2):
        return SubGraph(sub1.__nodes | sub2.__nodes, sub1.__relationships | sub2.__relationships)

    def __and__(sub1, sub2):
        return SubGraph(sub1.__nodes & sub2.__nodes, sub1.__relationships & sub2.__relationships)
    
    def __sub__(sub1, sub2):
        return SubGraph(sub1.__nodes - sub2.__nodes, sub1.__relationships - sub2.__relationships)

    def __xor__(sub1, sub2):
        return SubGraph(sub1.__nodes ^ sub2.__nodes, sub1.__relationships ^ sub2.__relationships)

