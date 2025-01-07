import os
import re
import sys
import yaml
import json
import psutil
import deepdiff
import argparse
import subprocess
import pandas as pd


def sorted_nicely( l ): 
    # credit: https://stackoverflow.com/a/2669120/5683778
    convert = lambda text: int(text) if text.isdigit() else text 
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ] 
    return sorted(l, key = alphanum_key)

def loadDict(file):
    out = None
    if os.path.isfile(file):
        if file.endswith('.yml'):
            with open(file) as f:
                out = yaml.safe_load(f)
        elif file.endswith('.json'):
            with open(file) as f:
                out = json.load(f)
    else:
        print(f"{file} does not exist")
    return(out)

def saveDict(obj,outputPath):
    if not os.path.isdir(os.path.split(outputPath)[0]):
        os.makedirs(os.path.split(outputPath)[0])
    with open(outputPath,'w') as file:
        if outputPath.endswith('.yml'):
            yaml.safe_dump(obj,file,sort_keys=False)
        if outputPath.endswith('.json'):
            json.dump(obj,file)

def repForbid(txt):
    # remove problematic characters from filepaths
    for rep in [' ','/','\\']:
        txt = txt.replace(rep,'_')
    for rep in [':','.']:
        txt = txt.replace(rep,'')
    return(txt)

# template/default exclusion patter for compareDicts
def exclude_ignore_callback(obj, path):
    # Exclude any dictionary where it contains a key:value pair 'ignore': True
    # Not recursive
    if isinstance(obj, dict) and obj.get('ignore') is True:
        return True
    return False

def compareDicts(new_dict,old_dict,ignore_order=True,exclude_keys=[],exclude_values=exclude_ignore_callback):
    # a wrapper on the deepdiff algorithm 
    # outputs changes in a format which is easier to interpret as a yaml file
    dd = deepdiff.DeepDiff(old_dict,new_dict,ignore_order=ignore_order,exclude_regex_paths=exclude_keys,exclude_obj_callback=exclude_values)
    if dd == {}:
        return(False)
    dDict = {}
    for key,diff in dd.items():
        if key == 'values_changed':
            dDict[key] = {}
            for root,data in diff.items():
                data['acceptNew'] = True
                keyNest = root.replace("root['",'').rstrip("']")
                logItem = packDict(keyNest,format="']['",fill=data)
                dDict[key] = updateDict(dDict[key],logItem)
        elif key == 'type_changes':
            dDict[key] = {}
            for root,data in diff.items():
                data['acceptNew'] = True
                data['old_type'] = str(data['old_type'])
                data['new_type'] = str(data['new_type'])
                keyNest = root.replace("root['",'').rstrip("']").replace("']['",'~')
                logItem = packDict(keyNest,format="']['",fill=data)
                dDict[key] = updateDict(dDict[key],logItem)
        elif key == 'dictionary_item_added':
            logItem = {}
            for item in diff:
                keyNest = item.replace("root['",'').rstrip("']").replace("']['",'~')
                logItem[keyNest] = findNestedValue(keyNest,new_dict,delimiter='~')
            dDict[key] = packDict(logItem,format='~')
        elif key == 'dictionary_item_removed':
            logItem = {}
            for item in diff:
                keyNest = item.replace("root['",'').rstrip("']").replace("']['",'~')
                logItem[keyNest] = findNestedValue(keyNest,old_dict,delimiter='~')
            dDict[key] = packDict(logItem,format='~')
        else:
            sys.exit(f'New for dict compare: {key}')
    return(dDict)
    
def unpackDict(Tree,format=os.path.sep,limit=None):
    # recursive function to condense a nested dict by concatenating keys to a string
    def unpack(child,parent=None,root=None,format=os.path.sep,limit=None):
        pth = {}
        if type(child) is dict and (limit is None or limit >= 0) and child:
            if limit is not None:
                limit -= 1
            for key,value in child.items():
                if parent is None:
                    pass
                else:
                    key = format.join([parent,key])
                if type(value) is not dict or (limit is not None and limit < 0) or not value:
                    pth[key] = unpack(value,key,root,format,limit)
                else:
                    pth = pth | unpack(value,key,root,format,limit)
        else:
            if type(child) is not dict or (limit is not None and limit < 0) or not child:
                return(child)
            else:
                sys.exit('Error in file tree unpack')
        return(pth)
    return(unpack(Tree,format=format,limit=limit))

def packDict(itemList,format=os.path.sep,limit=None,order=-1,fill=None):
    Tree = {}
    if type(itemList) is list:
        if fill == 'key':
            itemList = {key:key for key in itemList}
        elif type(fill) is list:
            itemList = {key:f for key,f in zip(itemList,fill)}
        else:
            itemList = {key:fill for key in itemList}
    elif type(itemList) is not dict:
        itemList = {itemList:fill}
    for key,value in itemList.items():
        b = key.split(format)
        if order == -1:
            if limit is None: lm = len(b)+order
            else: lm = limit
            start = len(b)
            end = max(0,len(b)+lm*order+order)
            for i in range(start,end,order):
                if i == start:
                    subTree = {b[i+order]:value}
                elif i>end+1:
                    subTree =  {b[i+order]:subTree}
                else:
                    subTree = {format.join(b[:i]):subTree}
        else:
            if limit is None: lm = len(b)
            else: lm = limit
            start = 0
            end = min(lm+1,len(b))
            for i in range(start,end,order):
                if i == start:
                    subTree = {format.join(b[end-1:]):value}
                else:
                    subTree = {b[end-1-i]:subTree}
        Tree = updateDict(Tree,subTree,overwrite='append')
    return(Tree)

def findNestedValue(element,nest,delimiter=os.path.sep):
    keys = element.split(delimiter)
    rv = nest
    for key in keys:
        rv = rv[key]
    return rv

def updateDict(base,new,overwrite=False):
    for key,value in new.items():
        if type(base) is dict and key not in base.keys():
            base[key]=value
        elif type(value) is dict and type(base[key]) is dict:
            base[key] = updateDict(base[key],value,overwrite)
        elif overwrite == True:
            base[key] = value
        elif overwrite == 'append' and type(base[key]) is list:
            if type(base[key][0]) is not list and type(value) is list:
                base[key] = [base[key]]
            base[key].append(value)
        elif overwrite == 'append' and type(base[key]) is not list:
            base[key] = [base[key]]
            base[key].append(value)
        elif base[key] is None:
            base[key] = value
        else:
            print(base[key],value)
            sys.exit('fix update exception!')
    return(base) 

def lists2DataFrame(**kwargs):
    df = pd.DataFrame(data = {key:val for key,val in kwargs.items()})
    if 'index' in df.columns:
        df = df.set_index('index',drop=True)
    return(df)

def str2bool(v):
    # credit: https://stackoverflow.com/a/43357954/5683778
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def getCMD(defaultArgs):
    CLI=argparse.ArgumentParser()
    dictArgs = []
    for key,val in defaultArgs.items():
        dt = type(val)
        nargs = "?"
        print(key,val,dt)
        if val == None:
            dt = str
        if dt == type({}):
            dictArgs.append(key)
            dt = type('')
            val = '{}'
        elif dt == type([]):
            nargs = '+'
            dt = type('')
        elif dt == type(False):
            dt = str2bool
        CLI.add_argument(f"--{key}",nargs=nargs,type=dt,default=val)

    # parse the command line
    args = CLI.parse_args()
    kwargs = vars(args)
    for d in dictArgs:
        kwargs[d] = json.loads(kwargs[d])
        # replace booleans
    print(kwargs)
    return(kwargs)

class progressbar():
    def __init__(self,items,prefix='',size=60,out=sys.stdout):
        self.nItems = items
        self.out = out
        self.i = 0
        self.prefix=prefix
        self.size=size
        self.msg = None
        self.show(0)

    def show(self,j):
        if self.nItems > 0:
            x = int(self.size*j/self.nItems)
            if self.msg is None:
                suffix = ""
            else:
                suffix = ' '+self.msg
            print(f"{self.prefix}[{u'█'*x}{('.'*(self.size-x))}] {j}/{self.nItems}{suffix} ", end='\r', file=self.out, flush=True)

    def step(self,step_size=1,msg=None,L=20):
        if msg is not None:
            self.msg = msg[-L:]
        self.i+=step_size
        self.show(self.i)

    def close(self):
        print('\n')

def set_high_priority():
    p = psutil.Process(os.getpid())
    p.nice(psutil.HIGH_PRIORITY_CLASS)

def pasteWithSubprocess(source, dest, option = 'copy',Verbose=False,pb=None):
    set_high_priority()
    cmd=None
    source = os.path.abspath(source)
    dest = os.path.abspath(dest)
    if sys.platform.startswith("darwin"): 
        # These need to be tested/flushed out
        if option == 'copy' or option == 'xcopy':
            cmd=['cp', source, dest]
        elif option == 'move':
            cmd=['mv',source,dest]
    elif sys.platform.startswith("win"): 
        cmd=[option, source, dest]
        if option == 'xcopy':
            cmd.append('/s')
    if cmd:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    if pb is not None:
        pb.step(msg=f"{source}")

    if Verbose==True:
        print(proc)

if __name__ == '__main__':
    prefix = 'Test'
    nItems=10
    pb = progressbar(nItems,prefix)
    for i in range(nItems):
        pb.step()
    pb.close()
