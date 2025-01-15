import os
import re
import sys
import copy
import datetime
import itertools
import numpy as np
import pandas as pd
from dataclasses import dataclass,field
import time

import importlib
from parseGHG import parseGHG
from parseTables import parseTOBA, parseHobo,parseMixedArray
importlib.reload(parseTOBA)
importlib.reload(parseHobo)
importlib.reload(parseMixedArray)
importlib.reload(parseGHG)

import helperFunctions as helper
importlib.reload(helper)

@dataclass
class project:
    # project path
    rootPath: str = os.path.join(os.getcwd(),'exampleProject')
    # list of defining the current instance of the project variables
    siteID: list = field(default_factory=list)
    Year: list = field(default_factory=lambda:[datetime.datetime.now().year])
    # elements of the current project
    aliases: dict = field(default_factory=dict)
    database: dict = field(default_factory=lambda:{'__Year__':{'__siteID__':{}}})
    rawData: dict = field(default_factory=lambda:{'__siteID__':{'files':{},'metadata':['fileInventory.json','groupID.json','metadata.yml','changeLog.yml']}})
    configFiles: dict = field(default_factory=lambda:{'__siteID__':{'test':['a.json','b.json']}})
    # project configurations
    Verbose: bool = False

    def __post_init__(self):
        for field_value in type(self).__mro__[-2].__dataclass_fields__.values():
            if field_value.type == list and not isinstance(self.__dict__[field_value.name],field_value.type):
                self.__dict__[field_value.name] = list([self.__dict__[field_value.name]])
            elif field_value.type == dict and field_value.name != 'aliases':
                self.aliases[field_value.name] = {}
                item = helper.unpackDict(self.__dict__[field_value.name])
                for keys,value in item.items():  
                    for key in self.subKeys(keys):
                        pth = os.path.join(self.rootPath,field_value.name,key)
                        if not os.path.isdir(pth):
                            os.makedirs(pth)
                        if type(value) is str:
                            if os.path.isfile(os.path.join(pth,value)):
                                ipt = helper.loadDict(os.path.join(pth,value))
                            else:
                                ipt = {}
                            self.aliases[field_value.name][key] = value
                            helper.updateDict(self.__dict__[field_value.name],helper.packDict({key:ipt}))  
                        elif type(value) is list:
                            for v in value:
                                nm = v.rsplit('.',1)[0]
                                if os.path.isfile(os.path.join(pth,v)):
                                    ipt = helper.loadDict(os.path.join(pth,v))
                                else:
                                    ipt = {}
                                helper.updateDict(self.__dict__[field_value.name],helper.packDict({os.path.join(key,nm):ipt}))
                            self.aliases[field_value.name][key] = value
                        else:
                            ipt = {}
                            helper.updateDict(self.__dict__[field_value.name],helper.packDict({key:ipt}))  

    def subKeys(self,key,keyList=None):
        if keyList is None: keyList = []
        pattern = rf"__(.+?)__"
        matches = re.findall(pattern, key)
        if len(matches) == 0: 
            keyList.append(key)
            return(keyList)
        for match in matches:
            attrs = getattr(self,match)
            for attr in attrs:
                keyList = self.subKeys(key.replace(f"__{match}__",str(attr)),keyList)
        return(keyList)
    
    def saveProject(self):
        for item, value in self.aliases.items():
            for pattern,name in value.items():
                if type(name) is str:
                    output = helper.findNestedValue(pattern,self.__dict__[item])
                    fpath = os.path.join(self.rootPath,item,pattern,name)
                    helper.saveDict(output,fpath)
                else:
                    for n in name:
                        patt = os.path.join(pattern,n.rsplit('.',1)[0])
                        output = helper.findNestedValue(patt,self.__dict__[item])
                        fpath = os.path.join(self.rootPath,item,pattern,n)
                        helper.saveDict(output,fpath)

@dataclass
class rawData(project):
    # Initializes a raw data import
    importRoot: str = ''
    importRelative: str = ''
    importFileList: list = field(default_factory=list)
    fileType: list = field(default_factory=list)
    includeTag: list = field(default_factory=list)
    excludeTag: list = field(default_factory=list)
    fileParsers: dict = field(default_factory=lambda:{
            'ghg':[parseGHG.parseGHG()],
            'dat':[parseTOBA.parseTOBA(),parseMixedArray.parseMixedArray()],
            'csv':[parseHobo.parseHoboCSV()]
                })
    
    def __post_init__(self):
        # search the input directory for files
        # cross reference against files which have already been imported
        super().__post_init__() 
        dataPath = os.path.join(self.importRoot,self.importRelative) 
        if len(self.siteID)>1:
            sys.exit('Multi-site raw file search currently not supported')
        if len(self.importFileList) == 0 and len(dataPath)>0:
            if not os.path.isdir(dataPath):
                sys.exit('Give valid input path/file')
            excludeList = [os.path.sep.join([self.importRoot]+val) if self.importRoot is not None else os.path.sep.join(val) 
                           for values in helper.unpackDict(self.rawData[self.siteID[0]]).values() for val in values]
            for curDir, _, fileList in os.walk(dataPath):
                self.importFileList += [file for file in [os.path.join(curDir,f) for f in fileList] if self.filter(file,excludeList)]
        elif len(self.importFileList)>0:
            self.importFileList = [f for f in self.importFileList if os.path.isfile(f)]
            if not len(self.importFileList):
                sys.exit('Give valid input path/file')
        self.importFileList = helper.sorted_nicely(self.importFileList)

    def filter(self,file,excludeList):
        keep = (file not in excludeList and
        sum(t in file for t in self.excludeTag) == 0 and 
        sum(t in file for t in self.includeTag) == len(self.includeTag) and
        file.rsplit('.',1)[1] in  self.fileParsers.keys() and
        (self.fileType == [] or file.rsplit('.',1)[1] in self.fileType))
        return keep
    
@dataclass
class fileInventory:
    name: str = None
    ID: str = None
    source: list = field(default_factory=list)
    entry: dict = field(default_factory=dict)
    def __post_init__(self):
        self.entry[self.name] = {
            'groupID':self.ID,
            'dataSource':self.source
        }
        self.entry = helper.packDict(self.entry)

class Parse(rawData):
    def __init__(self,mode='find',**kwds):
        T1 = time.time()
        super().__init__(**kwds)
        self.fileInventory = self.rawData[self.siteID[0]]['metadata']['fileInventory']
        self.groupID = self.rawData[self.siteID[0]]['metadata']['groupID']
        self.Metadata = self.rawData[self.siteID[0]]['metadata']['metadata']
        self.changeLog = self.rawData[self.siteID[0]]['metadata']['changeLog']
        if mode == 'find':
            self.readMetadata()
            self.copyFiles()
        elif mode == 'sync':
            print('fix here to create raw and corrected values')
            for table in self.changeLog.keys():
                for subType,Log in self.changeLog[table].items():
                    self.mdCorrect(Log,table,subType)
                    for combo in itertools.combinations(list(self.Metadata[table][subType].keys()),2):
                        combo = list(combo)
                        combo.sort()
                        if combo[0] in self.Metadata[table][subType].keys() and combo[1] in self.Metadata[table][subType].keys():
                            comp = self.compare(self.Metadata[table][subType][combo[1]],self.Metadata[table][subType][combo[0]])
                            if not comp:
                                self.Metadata[table][subType].pop(combo[1])
                                print('fix here')
                                for key in self.groupID[table][subType][combo[1]]:
                                    self.fileInventory[table][subType][key]['groupID'] = combo[0]
                                newValues = self.groupID[table][subType].pop(combo[1])
                                self.groupID[table][subType][combo[0]].append(newValues)
                                if self.Verbose: print('Removing',combo[1])
            self.saveProject()
        print('Parsing Completed in ', np.round(time.time()-T1,1),' seconds')

    def readMetadata(self):                
        print('Consider fixing to allow multi-site imports with one call?')
        for file in self.importFileList:
            for fileParser in self.fileParsers[file.rsplit('.')[-1]]:
                fileParser.parse(file)
                if fileParser.mode:
                    if self.importRoot is not None:
                        file = file.split(self.importRoot+os.path.sep)[-1]
                    filePath = file.split(os.path.sep)
                    typeKeys = ['Type']
                    fileType = '_'.join([helper.repForbid(value) for key,value in fileParser.Metadata.items() if key in typeKeys and value is not None])
                    subTypeKeys = ['Table','StationName']
                    fileSubType = '_'.join([helper.repForbid(value) for key,value in fileParser.Metadata.items() if key in subTypeKeys and value is not None])
                    idKeys = ['Timestamp']
                    ID = '_'.join([helper.repForbid(value) for key,value in fileParser.Metadata.items() if key in idKeys and value is not None])
                    fileMetadata = {}
                    fileMetadata['sourceInfo'] = copy.deepcopy(fileParser.Metadata)
                    fileMetadata['fileContents'] = copy.deepcopy(fileParser.Contents)                  
                    if fileType not in self.Metadata.keys():
                        self.Metadata[fileType] = {}    
                        self.changeLog[fileType] = {}    
                    if fileSubType not in self.Metadata[fileType].keys():
                        self.Metadata[fileType][fileSubType] = {}    
                        self.changeLog[fileType][fileSubType] = {}    
                        self.Metadata[fileType][fileSubType][ID] = fileMetadata
                        self.changeLog[fileType][fileSubType][ID] = None
                    else:
                        compare_to = list(self.Metadata[fileType][fileSubType].keys())
                        # compare with all existing metadata for the given file type
                        # work backwards from most recent
                        change = False
                        for old_ID in compare_to[::-1]:
                            old_dict = self.Metadata[fileType][fileSubType][old_ID]
                            if old_dict == fileParser.Contents:
                                ID,change = old_ID,False
                                break
                            else:
                                comp = self.compare(fileMetadata,old_dict)
                                if not change:
                                    change = comp
                                    if not change:
                                        ID,change = old_ID,False
                                        break
                        if change:
                            self.Metadata[fileType][fileSubType][ID] = copy.deepcopy(fileMetadata)
                            self.changeLog[fileType][fileSubType][ID] = copy.deepcopy(change)
                    if fileType in self.fileInventory.keys() and fileSubType in self.fileInventory[fileType].keys() and ID in self.Metadata[fileType][fileSubType].keys():
                        tmp = fileInventory(name=os.path.join(fileType,fileSubType,filePath[-1]),ID=ID,source=filePath)
                        helper.updateDict(self.fileInventory,tmp.entry,overwrite='append')
                        idOut = helper.packDict({os.path.split(tmp.name)[0]+os.path.sep+tmp.ID:os.path.split(tmp.name)[1]})
                        helper.updateDict(self.groupID,idOut,overwrite='append')
                    else:
                        tmp = fileInventory(name=os.path.join(fileType,fileSubType,filePath[-1]),ID=ID,source=filePath)
                        helper.updateDict(self.fileInventory,tmp.entry,overwrite='append')
                        idOut = helper.packDict({os.path.split(tmp.name)[0]+os.path.sep+tmp.ID:os.path.split(tmp.name)[1]})
                        helper.updateDict(self.groupID,idOut,overwrite='append')
                    break

    def mdCorrect(self,log,table,subType):
        print(log)
        print(table)
        print(subType)
        for incoming,comp in log.items():
            print(incoming,comp)
            if comp is None:
                pass
            else:
                if 'values_changed' in comp.keys() and incoming in self.Metadata[table][subType].keys():
                    self.base = helper.unpackDict(self.Metadata[table][subType][incoming])
                    self.makeChange(helper.unpackDict(comp['values_changed']))
                    self.Metadata[table][subType][incoming] = helper.packDict(self.base)
                if 'dictionary_item_added' in comp.keys():
                    Adds = comp['dictionary_item_added']
                    if self.Verbose: print('dictionary_item_added: ',Adds)
                if 'dictionary_item_removed' in comp.keys():
                    Rems = comp['dictionary_item_removed']
                    if self.Verbose: print('dictionary_item_removed: ',Rems)

    def makeChange(self,changes):
        keys = list(changes.keys())
        self.chg = False
        for new,old,accept in zip(*[iter(keys)]*3):
            if not changes[accept]:
                # Overwrite "erroneous" change picked up with autodetection with old value
                self.chg = True
                if self.Verbose: print('overwriting ',old.rsplit(os.path.sep,1)[0], ' of ',changes[new],' with ',changes[old])
                self.base[old.rsplit(os.path.sep,1)[0]]= changes[old]
            elif self.base[new.rsplit(os.path.sep,1)[0]]!= changes[new]:
                # Overwriting with user defined values which diverge from an autodetected change
                self.chg = True
                self.base[new.rsplit(os.path.sep,1)[0]]= changes[new]
                if self.Verbose: print('overwriting ',new.rsplit(os.path.sep,1)[0],' with ',changes[new])
            else:
                if self.Verbose: print('Accepting changes in ',old.rsplit(os.path.sep,1)[0], ' from ',changes[old],' to ',changes[new])

    def copyFiles(self):
        self.fileInventory = helper.unpackDict(self.fileInventory,limit=1)
        for subdir,fileInfo in self.fileInventory.items():
            for file,info in fileInfo.items():
                source = os.path.sep.join(info['dataSource'])
                if self.importRoot is not None:
                    source = os.path.join(self.importRoot,source)
                dest = os.path.join(self.rootPath,'rawData',self.siteID[0],'files',subdir)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                helper.pasteWithSubprocess(source,dest)
        self.saveProject()

    def compare(self,new_dict,old_dict,ignore_key_template = 'ghg'):
        if ignore_key_template == 'ghg':
            #Some GHG metadata will flag "false" change if not ignored
            ignore_key_template = [r"root.*\['Timestamp']\.*",r"root.*\['canopy_height']\.*",r"root.*\['altitude']\.*",
                r"root.*\['latitude']\.*",r"root.*\['longitude']\.*",r"root.*\_tube_flowrate']\.*",r"root.*\_timelag']\.*"]
        comp = helper.compareDicts(new_dict,old_dict,exclude_keys=ignore_key_template)
        return(comp)

@dataclass
class rawImport(rawData):
    firstStage: dict = field(default_factory=dict)

    def __post_init__(self):
    #     # search the input directory for files
    #     # cross reference against files which have already been imported
        super().__post_init__() 


class processing():
    def __init__(self,stage=None,**kwds):
        if stage is None:
            self = project(**kwds)
        elif stage == 'parse':
            self.Parse = Parse(**kwds)
        elif stage == 'import':
            self = rawImport(**kwds)

# class syncMetadata():
#     def __init__(self, projectPath, defaultArgs={ 'siteID': None, 'Verbose':False}, **kwargs):
#         super().__init__(projectPath, defaultArgs, **kwargs)
#         if self.siteID:
#             rd = self.projectConfig['Database']['rawData'][self.siteID] = {}
#             # Dump the first metadata file for each table
#             for table,view in self.projectView[self.siteID].changeLog.items():
#                 with open(os.path.join(self.Path['rawData'],self.siteID,table,'metadata.yml')) as f:
#                     out = yaml.safe_load(f)
#                 if table not in self.projectView[self.siteID].currentMetadata.keys():
#                     self.projectView[self.siteID].currentMetadata[table] = out[list(out.keys())[0]]
#                 self.projectView[self.siteID].tableName = table
#                 rd[table] = self.firstStage(self.projectView[self.siteID].currentMetadata[table])
#                 rd[table] = self.mdAdjust(rd[table],view,table)
    
#     def firstStage(self,md):
#         processing = {}
#         template = self.projectConfig['Database']['configFiles']['firstStage']['defaultVariable']
#         for name,item in md['fileContents'].items():
#             processing[name] = copy.deepcopy(template)
#             if md['Type'] != 'MixedArray':
#                 for key in template.keys():
#                     if key == 'date_range':
#                         processing[name][key] = [md['Timestamp']]
#                     elif key in item.keys():
#                         processing[name][key] = [item[key]]
#                     else:
#                         processing[name][key] = [template[key]]
#             else:
#                 for ar,it in item.items():
#                     for key in template.keys():
#                         if key == 'date_range':
#                             processing[name][key] = [md['Timestamp']]
#                         elif key in item.keys():
#                             processing[name][key] = [it[key]]
#                         else:
#                             processing[name][key] = [template[key]]
#         return(processing)

#     def mdAdjust(self,rd,log,table):
#         for incoming,comp in log.items():
#             if comp is None:
#                 pass
#             else:
#                 print(comp)
#         return(rd)
    
#     def mdCorrect(self,log,table):
#         for incoming,comp in log.items():
#             if comp is None:
#                 pass
#             else:
#                 if 'values_changed' in comp.keys():
#                     # currentMetadata
#                     # self.base = helper.unpackDict(self.projectView[self.siteID].allMetadata[table][incoming])
#                     self.base = helper.unpackDict(self.projectView[self.siteID].allMetadata[table])
#                     self.ComparedWith = comp['ComparedWith']
#                     if self.Verbose:print('comp: ',self.ComparedWith)
#                     values_changed = comp['values_changed']
#                     unpk = helper.unpackDict(values_changed)
#                     self.makeChange(unpk)
#                     self.projectView[self.siteID].allMetadata[table][incoming] = helper.repackDict(self.base)
#                 if 'dictionary_item_added' in comp.keys():
#                     Adds = comp['dictionary_item_added']
#                     print('dictionary_item_added')
#                     print(Adds)
#                 if 'dictionary_item_removed' in comp.keys():
#                     Rems = comp['dictionary_item_removed']
#                     print('dictionary_item_removed')
#                     print(Rems)


#     def makeChange(self,changes):
#         keys = list(changes.keys())
#         self.chg = False
#         for new,old,accept in zip(*[iter(keys)]*3):
#             if not changes[accept]:
#                 # Overwrite "erroneous" change picked up with autodetection with old value
#                 self.chg = True
#                 if self.Verbose: print('overwriting ',old.rsplit('.',1)[0], ' of ',changes[old],' with ',changes[old])
#                 self.base[old.rsplit('.',1)[0]] = changes[old]
#             elif self.ComparedWith == 'self':
#                 # Overwriting with user defined values where no change detected
#                 self.chg = True
#                 if self.Verbose: print('overwriting ',new.rsplit('.',1)[0], ' of ',self.base[new.rsplit('.',1)[0]],' with ',changes[new])
#                 self.base[new.rsplit('.',1)[0]] = changes[new]
#             elif self.base[new.rsplit('.',1)[0]] != changes[new]:
#                 # Overwriting with user defined values which diverge from an autodetected change
#                 chg = True
#                 self.base[new.rsplit('.',1)[0]] = changes[new]
#                 if self.Verbose: print('overwriting ',new.rsplit('.',1)[0],' with ',changes[new])
#             else:
#                 if self.Verbose: print('Accepting auto-detected changes in ',old.rsplit('.',1)[0], ' from ',changes[old],' to ',changes[new])






