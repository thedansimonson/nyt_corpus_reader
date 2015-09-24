"""
An NYT corpus reader. 
 by Dan Simonson       

Recommended: use ez_reader
"""


import tarfile
import os
import shutil
from xml.etree.ElementTree import XML
from pprint import pprint

all_months = [s.zfill(2) for s in map(str, range(1,13))]

def corpus_reader(path, selected_dates = {}):
    """
    Returns a generator for an NYT corpus reader.
    RECOMMENDED: Use ez_reader

    selected_dates = {year: [months...], year: [months...], ...}
    I know that format sucks but it makes my life easier.
    """
    path, years, empty = os.walk(path).next()
    
    selected_years = list(selected_dates) 

    #select only the years we're interested in, if available
    if selected_years: 
        years = filter(lambda y: y in selected_years, years)
    #default behavior is all years
    
    for year in years:
        y_path = os.sep.join([path, year])
        y_path, empty, archives = os.walk(y_path).next()
         
        selected_months = selected_dates[year] if selected_dates else all_months
        
        #similar to year selector
        if selected_months: 
            selected_months = ["".join([m, ".tgz"]) for m in selected_months]
            print selected_months
            archives = filter(lambda y: y in selected_months, archives)
        #except it formats each month as a tgz archive
        
        for archive in archives:
            #open a temp directory for the archive
            cwd = os.getcwd()
            tmp_path = os.sep.join([cwd, "temp"])
            try: shutil.rmtree(tmp_path)    #try to delete old temp dir
            except: pass
            os.mkdir(tmp_path)
            
            #extract to temp location
            open_arch = tarfile.open(os.sep.join([y_path, archive]))
            open_arch.extractall(path=tmp_path)
            
            #crawl temp location
            subpath, dirs, empty = os.walk(tmp_path).next()
            subpath = os.sep.join([subpath, dirs[0]])
            subpath, subdirs, empty = os.walk(subpath).next()
            subdirs = [os.sep.join([subpath, d]) for d in subdirs]

            for subdir in subdirs:
                blah, bleh, files = os.walk(subdir).next()
                filepaths = [os.sep.join([subdir, f]) for f in files]
                for filepath in filepaths:
                    yield open(filepath).read()


            #remove temp directory
            shutil.rmtree(tmp_path)

def merge_text(tl): 
    text_list = [t.text and t.text or "" for t in tl]
    #for t in text_list: print t
    return "\n\n".join(text_list)

def unpacker(string):
    """
    Parses XML and unpacks it into a Python dictionary.
    I RECOMMEND USING ez_reader() (SEE BELOW)

    Params:
        string - XML string from NYT corpus to extract from

    As of 2012-10-28, extracts:
        all <meta> tags - returned by their name attribute
        all <classifier> tags - returned as a list by their class attribute
        all text - returned as a string, all <p> tags joined by \\n\\n
    """
    tree = XML(string)
    output = {}
    
    #simple, collapsing extractions
    simple_extractions = [  #("text", ".//p"), #old text extract (too simple)
                            ("hedline",".//hedline/*"),
                            ("title","./head/title")
                            ]
    output.update([(label, merge_text(tree.findall(extract)))\
            for label, extract in simple_extractions])
    
    #get full text as "text"
    def grab_text(texts):
        "Gets all the damn p tags out."
        return merge_text(sum([t.findall(".//p") for t in texts],[]))

    text_blox = tree.findall(".//block")
    preferred = [b for b in text_blox if b.attrib["class"] == "full_text"]
    if preferred:
        output["text"] = grab_text(preferred)
    elif text_blox:
        #print output["hedline"],"failed to find preferred blocks." 
        #print "Using all blocks."
        output["text"] = grab_text(text_blox)
    else:
        #print output["hedline"],"failed to find blocks." 
        #print "Falling back to *all* available p tags."
        output["text"] = grab_text([tree])
        


    #classifiers
    class_tags = tree.findall(".//classifier")
    c_classes = set([c.attrib["class"] for c in class_tags])
    for class_type in c_classes:
        classy = lambda t: t.attrib["class"] == class_type
        output[class_type] = [c.text for c in filter(classy, class_tags)]
    
    #metadata
    for meta_tag in tree.findall(".//meta"):
        stuff = meta_tag.attrib
        output[stuff["name"]] = stuff["content"]
    
    #doc-id
    output["doc-id"] = tree.findall(".//doc-id")[0].attrib["id-string"]

    #special tags for special things
    tags = [("people", "person"),
            ("locations", "location"),
            ("orgs", "org")]
    [output.update([(pl, [p.text for p in tree.findall(".//"+sg)])])\
        for pl, sg in tags]
    
    return output

def ez_reader(*args):
    """
    A generator that combines the unpacker and corpus_reader in one package.

    THIS IS THE FUNCTION YOU WANT TO USE.

    Args:
        path: location of the 
        selected_dates = dictionary of {year: [months...], 
                                        year: [months...], ...}
                         if {}, then it does everything
            
    Example:
        Everything:
            ez_reader("/home/corpora/NYT/data/", {})
        September 1998 -> April 1999:
            ez_reader("/home/corpora/NYT/data/", 
                        selected_dates = {1998: [9,10,11,12], 1999: [1,2,3,4]})
    """
    reader = corpus_reader(*args)
    for each in reader:
        yield unpacker(each)



