#!/usr/bin/python

from neo4j import GraphDatabase
import py4j.SubGraph as SubGraph
import py4j.DataType as DataType
import py4j.Match as Match
import py4j.functions as functions

def propsUnwindStr(propNames, varName, prefix=""):
    strOut = ""
    for propName in propNames:
        strOut += "%s:%s.%s%s," % (propName, varName, prefix, propName)
    return strOut[:-1]

class Graph:
    def __init__(self, uri:str, name:str, user:str, password:str):
        self.__uri = uri
        self.__user = user
        self.__pass = password
        self.__name = name
        self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pass))

    def __repr__(self):
        return "URI: %s - DB: %s" % (self.__uri, self.__name)

    ########################################################################
    ##################NODE AND RELATIONSHIP CREATION########################

    #cambiar el merge agregando el uso del ID cuando esta presente en nodos y en relaciones 
    def __createMergeNodes(self, subgraph, operation):
        with self.__driver.session(database=self.__name) as session:
            separatedNodes = subgraph.separateNodes()
            #nodeData[0] -> Tuple of label names
            #nodeData[1] -> Tuple of properties names
            for nodeData in separatedNodes:
                strCM  = "WITH $nodeList as batch\n"
                strCM += "UNWIND batch AS node\n"
                strCM += "%s (n:%s {%s})" % (operation, ":".join(nodeData[0]), propsUnwindStr(nodeData[1], "node"))
                session.run(strCM, nodeList=separatedNodes[nodeData])

    def __createMergeRelationships(self, subgraph, operation):
        with self.__driver.session(database=self.__name) as session:
            separatedRels = subgraph.separateRelationships()
            #relData[0] -> Start node data: Tuple((labelNames), (propNames))
            #relData[1] -> Relation data: Tuple((typeName), (propNames)) 
            #relData[2] -> End node data: Tuple((labelNames), (propNames)) 
            for relData in separatedRels:
                strCM  = "WITH $relList as batch\n"
                strCM += "UNWIND batch AS rel\n"
                strCM += "%s (start:%s {%s})\n" % (operation, ":".join(relData[0][0]), propsUnwindStr(relData[0][1], "rel", "start_"))
                strCM += "%s (end:%s {%s})\n" % (operation, ":".join(relData[2][0]), propsUnwindStr(relData[2][1], "rel", "end_"))
                strCM += "%s (start)-[r:%s {%s}]->(end)" % (operation, ":".join(relData[1][0]), propsUnwindStr(relData[1][1], "rel", "rel_"))
                session.run(strCM, relList=separatedRels[relData])
    
    def create(self, subgraph):
        self.__createMergeNodes(subgraph, "CREATE")
        self.__createMergeRelationships(subgraph, "CREATE")
   
    def merge(self, subgraph):
        self.__createMergeNodes(subgraph, "MERGE")
        self.__createMergeRelationships(subgraph, "MERGE")

    ########################################################################
    ########################################################################

    ########################################################################
    ##################NODE AND RELATIONSHIP MATCHING########################
    def nodeMatch(self, *labels, **props):
        nodes = None
        with self.__driver.session(database=self.__name) as session:
            nodes = session.run("MATCH (n%s {%s}) RETURN n" % (":"+":".join(labels) if labels else "", functions.dicToProps(props)))
            nodes = Match.Match(tuple(nodes.value("n")))
        return nodes

    def relationshipMatch(self, startNode, endNode, typeN=None, **props):
        rels = None
        if startNode == None: startNode = DataType.Node()
        if endNode == None: endNode = DataType.Node()
        with self.__driver.session(database=self.__name) as session:
            rels = session.run("MATCH (s%s {%s})-[r%s {%s}]->(e%s {%s}) RETURN s,e,r" % (":"+startNode.strLabels() if startNode.labels() else "", startNode.strProps(), ":"+typeN if typeN else "", functions.dicToProps(props), ":"+endNode.strLabels() if endNode.labels() else "", endNode.strProps()))
            rels = Match.Match(tuple(rels.value("r")))
        return rels
    ########################################################################
    ########################################################################
   
    def run(self, cypherQuery, keyValue = None):
        results = None
        with self.__driver.session(database=self.__name) as session:
            results = session.run(cypherQuery)
            if value != None:
                results = results.value(keyValue) #Lista con resultados de un key value especifico
            else:
                results = results.values() # Lista de listas con todos los resultados
        return results

    ########################################################################
    ########################INDEXES AND CONSTRAINTS#########################
    def __createRule(self, ruleType, dataType, labelType, *props):
        dataSection = "(r:%s)" if dataType == "Node" else "()-[r:%s]-()"
        propSection = "ON (%s)" if ruleType == "INDEX" else "REQUIRE (%s) IS UNIQUE"
        createRuleLine = "CREATE %s IF NOT EXISTS FOR %s %s" % (ruleType, dataSection, propSection) 
        with self.__driver.session(database=self.__name) as session:
                session.run(createRuleLine % (labelType, ",".join(("r."+prop for prop in props))))

    def createNodeIndex(self, label, *props):
        if label and props: self.__createRule("INDEX", "Node", label, *props)
    def createRelationshipIndex(self, relType, *props):
        if relType and props: self.__createRule("INDEX", "Rel", relType, *props)

    #Constraints creation implies indexes creation
    def createUniqNodeConstraint(self, label, *props):
        if label and props: self.__createRule("CONSTRAINT", "Node", label, *props)
    def createUniqRelationshipConstraint(self, relType, *props):
        #Constraint creation implies index creation
        if relType and props: self.__createRule("CONSTRAINT", "Rel", relType, *props)

    def __getRuleName(self, ruleType, labelType, *props):
        ruleType = "INDEXES" if ruleType=="INDEX" else "CONSTRAINTS"
        ruleName = None
        with self.__driver.session(database=self.__name) as session:
            ruleName = session.run("SHOW %s WHERE labelsOrTypes=['%s'] AND properties=['%s']" % (ruleType, labelType, "','".join(props))).value("name")
            ruleName = ruleName[0] if ruleName else None
        return ruleName

    def getIndexName(self, labelType, *props):
        if labelType and props:
            return self.__getRuleName("INDEX", labelType, *props)
    def getUniqConstraintName(self, labelType, *props):
        if labelType and props:
            return self.__getRuleName("CONSTRAINT", labelType, *props)

    def __dropRule(self, ruleType, labelType, *props):
        ruleName = self.__getRuleName(ruleType, labelType, *props)
        if ruleName != None:
            with self.__driver.session(database=self.__name) as session:
                session.run("DROP %s %s" % (ruleType, ruleName))

    #cannot remove index related to constraint
    def dropIndex(self, labelType, *props):
        if labelType and props:
            self.__dropRule("INDEX", labelType, *props)
    def dropUniqConstraint(self, labelType, *props):
        if labelType and props:
            self.__dropRule("CONSTRAINT", labelType, *props)

    def __getRules(self, ruleType, labelType):
        ruleType = "INDEXES" if ruleType=="INDEX" else "CONSTRAINTS"
        rules = None
        allProps = []
        with self.__driver.session(database=self.__name) as session:
            indexes = session.run("SHOW %s WHERE labelsOrTypes IS NOT NULL" % (ruleType))
            for labelOrType,props in indexes.values("labelsOrTypes","properties"):
                if labelType == None or labelType in labelOrType:
                    allProps.append(props)
        return tuple(map(tuple,allProps)) if allProps else None

    def getIndexes(self, labelType = None):
        return self.__getRules("INDEX", labelType)
    def getUniqConstraints(self, labelType = None):
        return self.__getRules("CONSTRAINT", labelType)
    ########################################################################
    ########################################################################

