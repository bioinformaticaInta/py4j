#!/usr/bin/python

import neo4j
import py4j.DataType as DataType
import py4j.functions as functions

##############################################################################
##################################MATCH#######################################
class Match:
    def __init__(self, neo4jResults):
        self.__results = neo4jResults
        self.__function = None
        if neo4jResults:
            if isinstance(neo4jResults[0], neo4j.graph.Node):
                self.__function = DataType.Node.fromNeo4jData
            else:
                self.__function = DataType.Relationship.fromNeo4jData

    def __len__(self):
        return len(self.__results)

    def __repr__(self):
        return str(self.__results)

    def first(self):
        return self.__function(self.__results[0]) if self.__results else None

    def all(self):
        return tuple(map(self.__function, self.__results))

    def __iter__(self):
        return IterMatch(self)
##############################################################################
##############################################################################

##############################################################################
##############################MATCH ITERATOR##################################
class IterMatch:
    def __init__(self, results):
        self.__results = results._Match__results
        self.__function = results._Match__function
        self.__posActual = 0
        self.__actual = None

    def __next__(self):
        if self.__posActual < len(self.__results): 
            self.__actual = self.__function(self.__results[self.__posActual])
            self.__posActual += 1
            return self.__actual
        else:
            raise StopIteration("End of tuple")
##############################################################################
##############################################################################
