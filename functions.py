#!/usr/bin/python

def dicToProps(dicProps):
    propStr = []
    for propName, propValue in dicProps.items():
        try:
            propStr.append(propName + ":" + "'"+propValue+"'")
        except:
            propStr.append(propName + ":" + str(propValue))
    return ",".join(propStr)
