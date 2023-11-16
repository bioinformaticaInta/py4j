#!/usr/bin/python

import py4j.functions as functions

##############################################################################
###############################MAIN CLASS#####################################
class DataType:
    def __init__(self, **props):
        self.__props = props        #Dict of properties
        self.__id = None            #ID in database

    def hasProperty(self, prop):
        return prop in self.__props
    
    def hasProperties(self, **props):
        return len(props)==len(set(props.items())&set(self.__props.items()))

    def getID(self):
        return self.__id

    def setID(self, idN):
        self.__id = idN

    def properties(self):
        return self.__props

    def propertiesForUnwind(self, prefix):
        return {prefix+"_"+propName:self.__props[propName] for propName in self.__props}

    def strProps(self):
        return functions.dicToProps(self.__props)

    def propNames(self):
        return tuple(self.__props.keys())

    def __dict__(self):
        return self.__props

    def __len__(self):
        return len(self.__props)

    def __getitem__(self, propName):
        propValue = None
        try:
            propValue = self.__props[propName]
        except:
            pass
        return propValue

    def __setitem__(self, propName, propValue):
        if propValue == None and self.hasProperty(propName):
            self.__props.pop(propName)
        else:
            self.__props[propName] = propValue
##############################################################################
##############################################################################

##############################################################################
###############################NODE CLASS#####################################
class Node(DataType):
    def __init__(self, *labels, **props):
        super().__init__(**props)
        self.__labels = labels      #Tuple of labels

    def __getstate__(self):
        return {"labels":self.__labels, "props":self._DataType__props, "id":self._DataType__id}
    
    def __setstate__(self, state):
        self.__labels = state["labels"]
        self._DataType__props = state["props"]
        self._DataType__id = state["id"]

    def fromNeo4jData(neo4jNode):
        node = Node(*neo4jNode.labels, **dict(neo4jNode.items()))
        node._DataType__id = neo4jNode.id
        return node
    
    def __repr__(self):
        return "(:%s {%s,id:%s})" % (':'.join(self.__labels), self.strProps(), self._DataType__id)

    def __hash__(self):
        nodeHash = hash(id(self))
        if self._DataType__id != None:
            nodeHash = hash(self._DataType__id)
        else:
            nodeHash = hash((self.__labels, tuple(self._DataType__props.items())))
        return nodeHash
    
    def __eq__(node1, node2):
        areEqual = False
        if isinstance(node1, Node) and isinstance(node2, Node):
            areEqual = id(node1)==id(node2)
            if not areEqual:
                if node1._DataType__id != None and node2._DataType__id != None:
                    areEqual = node1._DataType__id == node2._DataType__id
                else:
                    areEqual = node1.__labels==node2.__labels and node1._DataType__props==node2._DataType__props
        return areEqual

    def hasLabel(self, label):
        return label in self.__labels
    
    def hasLabels(self, *labels):
        return len(labels)==len(set(labels)&set(self.__labels))

    def isNode(self, *labels, **props):
        return self.hasLabels(*labels) and self.hasProperties(**props)

    def labels(self):
        return self.__labels

    def strLabels(self):
        return ":".join(self.__labels)
##############################################################################
##############################################################################

##############################################################################
###########################RELATIONSHIP CLASS#################################
class Relationship(DataType):
    def __init__(self, startNode:Node, endNode:Node, typeRel:str=None, **props):
        super().__init__(**props)
        self.__startNode = startNode           #Start Node
        self.__type = typeRel.upper()  #Type of relation
        self.__endNode = endNode               #End Node

    def __getstate__(self):
        return {"startNode":self.__startNode, "endNode":self.__endNode, "type":self.__type, "props":self._DataType__props, "id":self._DataType__id}
    
    def __setstate__(self, state):
        self.__startNode = state["startNode"]
        self.__endNode = state["endNode"]
        self.__type = state["type"]
        self._DataType__props = state["props"]
        self._DataType__id = state["id"]

    def fromNeo4jData(neo4jRelation):
        startNode = Node.fromNeo4jData(neo4jRelation.nodes[0])
        endNode = Node.fromNeo4jData(neo4jRelation.nodes[1])
        rel = Relationship(startNode, endNode, neo4jRelation.type, **dict(neo4jRelation.items()))
        rel._DataType__id = neo4jRelation.id
        return rel

    def __repr__(self):
        return "%s-[:%s {%s,id=%s}]->%s" % (self.__startNode, self.__type, self.strProps(), self._DataType__id, self.__endNode)

    def __hash__(self):
        hashRel = hash(id(self))
        if self._DataType__id != None:
            hashRel = hash(self._DataType__id)
        else:
            hashRel = hash((self.__startNode,self.__endNode,self.__type))
        return hashRel

    def __eq__(rel1, rel2):
        areEqual = False 
        if isinstance(rel1, Relationship) and isinstance(rel2, Relationship):
            areEqual = id(rel1)==id(rel2)
            if not areEqual:
                if rel1._DataType__id!=None and rel2._DataType__id!=None:
                    areEqual = rel1._DataType__id == rel2._DataType__id
                else:
                    areEqual = rel1.__startNode==rel2.__startNode and rel1.__endNode==rel2.__endNode and rel1.__type==rel2.__type
        return areEqual

    def type(self):
        return self.__type

    def isType(self, typeName):
        return self.__type == typeName

    def isEdge(self, typeName=None, **props):
        return (self.isType(typeName) if typeName else True) and self.hasProperties(**props)

    def nodes(self):
        return (self.__startNode, self.__endNode)

    def startNode(self):
        return self.__startNode

    def endNode(self):
        return self.__endNode
##############################################################################
##############################################################################
