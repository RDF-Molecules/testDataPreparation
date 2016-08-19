import random
import rdflib
from SPARQLWrapper import SPARQLWrapper, JSON, POST, N3
import requests as rq
from requests import Request, Session
import codecs
import sys
import json
reload(sys)
sys.setdefaultencoding("utf-8")

endpoint = "http://dbpedia.org/sparql"
dydra = "https://dydra.com/mgalkin/dbpedia_hierarchy/sparql"
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

#buildAdditionalDataset()
# STEP 3: Generate dumps with conflicts of 4 types

#subpropcache = dict()
with open('subpropcache.txt') as f:
  subpropcache = dict(x.rstrip().split(None, 1) for x in f)
superclasscache = dict()
broadercache = dict()
dctcache = dict()

def generateDumps(numdumps):

    gold = codecs.open("goldStandard.nt", "r")
    dumpFiles = []
    #g = rdflib.Graph()
    #g.parse("goldStandard.nt", format="n3")
    for i in xrange(0, numdumps):
        filename = "dump"+str(i)+".nt"
        dumpfile = codecs.open(filename,"a")
        dumpFiles.append(dumpfile)

    count = 1

    for line in gold:
        count += 1
        # if (count<745067):
        #     continue
        triple = line.split(" ")
        subject = triple[0]
        predicate = triple[1]
        object = ""
        if len(triple)==4:
            object = triple[2]
        else:
            for i in xrange(2,len(triple)-1):
                object += triple[i]+" "
        print object

        # Decide which conflict to introduce
        # 0 - replace predicate with subPropertyOf
        # 1 - replace object with superclass
        # 2 - replace object with skos:broader
        # 3 - replace object with dct:subject
        # 4 - do nothing

        toCopy = selectDumps(numdumps, dumpFiles)
        options = []
        if checkAvailable("subPropertyOf",predicate):
            options.append(0)
        if "\"" not in object:
            if checkAvailable("subClassOf",object):
                options.append(1)
            if ("Category" in object) or ("category" in object):
                if checkAvailable("broader", object):
                    options.append(2)
            if checkAvailable("dctsub", object):
                options.append(3)
        options.append(4)
        print options

        for dumpcopy in toCopy:
            choice = random.choice(options)
            print choice

            if choice == 0:
                newProp = getSubproperty(predicate)
                print subject+" "+newProp+" "+object+" .\n"
                dumpcopy.write(subject+" "+newProp+" "+object+" .\n")
            elif choice == 1:
                newObj = getSuperclass(object)
                print subject+" "+predicate+" "+newObj+" .\n"
                dumpcopy.write(subject+" "+predicate+" "+newObj+" .\n")
            elif choice == 2:
                newObj = getBroader(object)
                print subject+" "+predicate+" "+newObj+" .\n"
                dumpcopy.write(subject+" "+predicate+" "+newObj+" .\n")
            elif choice == 3:
                newObj = getDctsub(object)
                print subject+" "+predicate+" "+newObj+" .\n"
                dumpcopy.write(subject+" "+predicate+" "+newObj+" .\n")
            elif choice == 4:
                dumpcopy.write(line)


        print "%i triples processed"%count

        # Decide where to write new triple
        # Then generate k conflicts where k is the sampled number of dumps to write
    for dfile in dumpFiles:
        dfile.close()
    gold.close()
    return 0

def checkAvailable(toCheck,entity):

    if toCheck=="subPropertyOf":
        # querySubprop = """
        #         PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        #         ASK {
        #             %s rdfs:subPropertyOf ?p1.
        #         }
        #     """ % entity
        # gg = SPARQLWrapper(endpoint)
        # gg.setReturnFormat(JSON)
        # gg.setQuery(querySubprop)
        # results = gg.query().convert()
        #print results["boolean"]
        return entity in subpropcache
    elif toCheck=="subClassOf":
        if entity in superclasscache:
            if (superclasscache[entity] == False):
                return False
            else:
                return True
        else:
            try:
                querySuperclass = """
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    SELECT ?class {
                        %s rdfs:subClassOf+ ?class
                    }
                """ % entity
                gg = SPARQLWrapper(endpoint)
                gg.setReturnFormat(JSON)
                gg.setQuery(querySuperclass)
                results = gg.query().convert()

                if len(results["results"]["bindings"])==0:
                    superclasscache[entity] = False
                    return False
                else:
                    classes = []
                    for row in results["results"]["bindings"]:
                        classes.append("<"+row["class"]["value"]+">")
                    superclasscache[entity] = classes
                    return True
            except:
                superclasscache[entity] = False
    elif toCheck=="broader":
        if entity in broadercache:
            if (broadercache[entity] == False):
                return False
            else:
                return True
        else:
            try:
                queryBroader = """
                    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
                    SELECT ?b {
                        %s skos:broader ?b
                    }
                """ % entity.lower()
                gg = SPARQLWrapper(dydra)
                gg.setReturnFormat(JSON)
                gg.setQuery(queryBroader)
                results = gg.query().convert()
                if len(results["results"]["bindings"])==0:
                    broadercache[entity] = False
                    return False
                else:
                    classes = []
                    for row in results["results"]["bindings"]:
                        classes.append("<"+row["b"]["value"].replace("category","Category")+">")
                    broadercache[entity] = classes
                    return True
            except:
                broadercache[entity] = False
    elif toCheck=="dctsub":
        if entity in dctcache:
            if (dctcache[entity] == False):
                return False
            else:
                return True
        else:
            try:
                queryDctsubj = """
                    PREFIX dct: <http://purl.org/dc/terms/>
                    SELECT ?sub {
                        %s dct:subject ?sub
                    }
                """ % entity
                gg = SPARQLWrapper(endpoint)
                gg.setReturnFormat(JSON)
                gg.setQuery(queryDctsubj)
                results = gg.query().convert()
                if len(results["results"]["bindings"])==0:
                    dctcache[entity] = False
                    return False
                else:
                    classes = []
                    for row in results["results"]["bindings"]:
                        classes.append("<"+row["sub"]["value"]+">")
                    dctcache[entity] = classes
                    return True
            except:
                dctcache[entity] = False

def getSubproperty(prop):
    # query = """
    #     PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    #     SELECT ?p
    #     WHERE {
    #         %s rdfs:subPropertyOf+ ?p
    #     }
    # """ % prop
    # gg = SPARQLWrapper(endpoint)
    # gg.setReturnFormat(JSON)
    # gg.setQuery(query)
    # results = gg.query().convert()
    # props = []
    # for row in results["results"]["bindings"]:
    #     props.append("<"+row["p"]["value"]+">")
    # return random.choice(props)
    return subpropcache[prop]

def getSuperclass(ent):
    # query = """
    #     PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    #     SELECT ?class
    #     WHERE {
    #         %s rdfs:subClassOf ?class
    #     }
    # """ % ent
    # gg = SPARQLWrapper(endpoint)
    # gg.setReturnFormat(JSON)
    # gg.setQuery(query)
    # results = gg.query().convert()
    classes = superclasscache[ent]
    # for row in results["results"]["bindings"]:
    #     classes.append("<"+row["class"]["value"]+">")
    return random.choice(classes)

def getBroader(ent):
    # query = """
    #     PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    #     SELECT ?class
    #     WHERE {
    #         %s skos:broader ?class
    #     }
    # """ % ent
    # gg = SPARQLWrapper(dydra)
    # gg.setReturnFormat(JSON)
    # gg.setQuery(query)
    # results = gg.query().convert()
    classes = broadercache[ent]
    # for row in results["results"]["bindings"]:
    #     classes.append("<"+row["class"]["value"]+">")
    return random.choice(classes)

def getDctsub(ent):
    # query = """
    #     PREFIX dct: <http://purl.org/dc/terms/>
    #     SELECT ?class
    #     WHERE {
    #         %s dct:subject ?class
    #     }
    # """ % ent
    # gg = SPARQLWrapper(endpoint)
    # gg.setReturnFormat(JSON)
    # gg.setQuery(query)
    # results = gg.query().convert()
    classes = dctcache[ent]
    # for row in results["results"]["bindings"]:
    #     classes.append("<"+row["class"]["value"]+">")
    return random.choice(classes)

generateDumps(3)
