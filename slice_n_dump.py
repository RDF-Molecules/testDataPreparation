import random
import rdflib
from SPARQLWrapper import SPARQLWrapper, JSON, POST, N3
import requests as rq
from requests import Request, Session
import codecs
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

endpoint = "http://dbpedia.org/sparql"
def checkNumTriples():


    query = """
            PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>
            SELECT COUNT(?s) as ?num
            WHERE {
                ?s a dbpedia-owl:Person
            }
    """

    gg = SPARQLWrapper(endpoint)
    gg.setQuery(query)
    gg.setReturnFormat(JSON)
    results = gg.query().convert()
    print results["results"]["bindings"][0]["num"]["value"]
    return results["results"]["bindings"][0]["num"]["value"]

# 1760736 Persons

def getSubjects():
    gg = SPARQLWrapper(endpoint)
    gg.setReturnFormat(JSON)
    query2 = """
            PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>
            SELECT DISTINCT (?s)
            WHERE {
                ?s a dbpedia-owl:Person
            } LIMIT 10000 OFFSET %s
     """
    print query2
    subFile = codecs.open("subjects.txt","a","utf-8")
    for i in xrange(30, 178):
        offset = i*10000
        print "OFFSET ",offset
        querySubjects = query2 % offset
        gg.setQuery(querySubjects)
        results = gg.query().convert()
        for result in results["results"]["bindings"]:
            subject = result["s"]["value"]
            subFile.write("<"+subject+">\n")
    subFile.close()

def slicePropsForDumps(numdumps):
    subs = codecs.open("subjects.txt","r")

    goldStandard = codecs.open("goldStandard.nt","w")
    dumpFiles = []

    for i in xrange(0, numdumps):
        filename = "dump"+str(i)+".nt"
        dumpfile = codecs.open(filename,"w")
        dumpFiles.append(dumpfile)

    count = 1
    total = 1760736
    triplesCount = 0

    for line in subs:
        sub = line.rstrip('\n')
        print sub

        queryProps = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            SELECT ?p ?o ?type ?subject ?broader
            WHERE {
                %s ?p ?o.
                OPTIONAL {?o rdf:type ?type. }
                OPTIONAL {?o dct:subject ?subject. }
                OPTIONAL {?o skos:broader ?broader. }
            }
        """ % sub
        gg = SPARQLWrapper(endpoint)
        gg.setReturnFormat(JSON)
        gg.setQuery(queryProps)
        results = gg.query().convert()

        # previous prop and object to process multiple rows with the same prop and object but diff type
        previousProp = ""
        previousObj = ""
        same = False
        for row in results["results"]["bindings"]:
            prop = "<"+row["p"]["value"]+">"
            obj = parseObject(row["o"])
            # print prop, obj,"\n"

            if (previousProp=="" and previousObj==""):
                previousProp = prop
                previousObj = obj
            else:
                same = (previousObj == obj and previousProp == prop)
                previousProp = prop
                previousObj = obj

            toCopy = selectDumps(numdumps, dumpFiles)
            #print toCopy
            goldTriple = sub.encode("utf-8")+" "+prop.encode("utf-8")+" "+obj.encode("utf-8")
            #print triple

            # consider possible type of the object
            if 'type' in row:
                objtype = parseObject(row["type"])
                #print objtype
                typetriple = obj.encode("utf-8")+" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "+objtype.encode("utf-8")+" .\n"
                #print typetriple

            for dumpcopy in toCopy:
                name = dumpcopy.name
                nameToInclude = name.split(".")[0]
                subjectName = sub.split(">")
                triple = subjectName[0].encode("utf-8")+"/"+nameToInclude+"> "+prop.encode("utf-8")+" "+obj.encode("utf-8")
                if (same == True):
                    dumpcopy.write(typetriple)
                elif (same == False):
                    dumpcopy.write(triple+" .\n")
                    dumpcopy.write(typetriple)

            if (same == True):
                goldStandard.write(typetriple)
            elif (same == False):
                goldStandard.write(goldTriple+" .\n")
                goldStandard.write(typetriple)


        count += 1
        triplesCount += len(results["results"]["bindings"])

        if (count == 2):
            break
        print "%i subjects, num triples: %i"%(count,triplesCount)


    #close all files
    for dfile in dumpFiles:
        dfile.close()
    goldStandard.close()

    return 0

def parseObject(objString):
    if (objString["type"] == "uri"):
        obj = "<"+objString["value"]+">"
    elif (objString["type"] == "literal"):
        if ("xml:lang" in objString):
            obj = "\""+objString["value"].replace("\"","'").replace("\n",'')+"\"@"+objString["xml:lang"]
        else:
            obj = "\""+objString["value"].replace("\"","'").replace("\n",'')+"\""
    elif (objString["type"] == "typed-literal"):
        obj = "\""+objString["value"]+"\"^^<"+objString["datatype"]+">"
    elif (objString["type"] == "bnode"):
        obj = "\""+objString["value"]+"\""
    return obj

def selectDumps(num, dumpfiles):
    numDumpsSelect = random.choice(list(xrange(1, num+1)))
    return random.sample(dumpfiles,numDumpsSelect)



#slicePropsForDumps(3)

# STEP 1: Prepare a gold standard dataset with an additional dataset with more semantics
def buildGoldStandard():
    subs = codecs.open("subjects.txt","r")
    goldStandard = codecs.open("goldStandard.nt","w")
    count = 1
    total = 1760736
    triplesCount = 0

    for line in subs:
        sub = line.rstrip('\n')
        print sub

        queryProps = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            SELECT ?p ?o ?type ?subject ?broader
            WHERE {
                %s ?p ?o.
            }
        """ % sub
        gg = SPARQLWrapper(endpoint)
        gg.setReturnFormat(JSON)
        gg.setQuery(queryProps)
        results = gg.query().convert()

        for row in results["results"]["bindings"]:
            prop = "<"+row["p"]["value"]+">"
            obj = parseObject(row["o"])
            goldTriple = sub.encode("utf-8")+" "+prop.encode("utf-8")+" "+obj.encode("utf-8")+" .\n"
            goldStandard.write(goldTriple)


        count += 1
        triplesCount += len(results["results"]["bindings"])

        print "%i subjects, num triples: %i"%(count-1,triplesCount)
        if (count == 20001):
            break

    goldStandard.close()
    return 0

#buildGoldStandard()

# STEP 2: Prepare a dataset with additional semantics
def buildAdditionalDataset():
    subs = codecs.open("subjects.txt","r")

    additionalSemantics = codecs.open("additionalSem.nt",'w')

    count = 1
    total = 1760736
    triplesCount = 0

    for line in subs:
        sub = line.rstrip('\n')
        print sub

        queryProps = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            CONSTRUCT {
                ?o rdf:type ?type.
                ?o dct:subject ?subject.
                ?o skos:broader ?broader. }
            WHERE {
                %s ?p ?o.
                OPTIONAL {?o rdf:type ?type. }
                OPTIONAL {?o dct:subject ?subject. }
                OPTIONAL {?o skos:broader ?broader. }
            }
        """ % sub
        gg = SPARQLWrapper(endpoint)
        gg.setReturnFormat(JSON)
        gg.setQuery(queryProps)
        results = gg.query().convert()

        for row in results["results"]["bindings"]:
            prop = "<"+row["p"]["value"]+">"
            obj = parseObject(row["o"])
            subject = parseObject(row["s"])

            triple = subject.encode("utf-8")+" "+prop.encode("utf-8")+" "+obj.encode("utf-8")+" .\n"
            additionalSemantics.write(triple)

        count += 1
        triplesCount += len(results["results"]["bindings"])

        print "%i subjects, num triples: %i"%(count-1,triplesCount)
        if (count == 20001):
            break

    additionalSemantics.close()

    return 0

buildAdditionalDataset()
# STEP 3: Generate dumps with conflicts of 4 types
