import NYT
from pprint import pprint
import pickle

path = sys.argv[1] 

#ez_reader is generally preferred, but here I'm just looking for straight 
#strings
reader = NYT.ez_reader(path, {})

print reader

def ident(doc, keystrings):
    ""
    for ks in keystrings:
        if ks in doc["text"]:
            if "keywords" in doc: doc["keywords"].append(ks)
            else: doc["keywords"] = [ks]
    return doc

def dump(stuff, stuffs):
    ""
    print "Dumping "+`stuffs`
    fstream = open("../Outputs/0_raw-extracts/nyt-search_%s.pkl"%`stuffs`, "wb")
    pickle.dump(stuff, fstream)
    fstream.close()
        

keystrings = ["police"]

stuffs = 0
stuff_len = 10000
stuff = []

for doc in reader:
    doc = ident(doc, keystrings)
    stuff.extend([] if "keywords" not in doc else [doc])
    
    if len(stuff) % (stuff_len / 10) == 0: print len(stuff)
    if len(stuff) > stuff_len:
        #it's happening again
        
        dump(stuff, stuffs)

        stuffs += 1 
        stuff = []

dump(stuff, stuffs)

        
