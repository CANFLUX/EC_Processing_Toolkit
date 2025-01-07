import os
import re
import sys
import copy
import json
import yaml
import time
import shutil
import fnmatch
import getpass
import datetime
import itertools
import numpy as np
import pandas as pd
import deepdiff
import importlib
from parseTables import parseTOBA, parseHobo,parseMixedArray
from parseGHG import parseGHG
# import projectView
importlib.reload(parseTOBA)
importlib.reload(parseHobo)
importlib.reload(parseMixedArray)
importlib.reload(parseGHG)
# importlib.reload(projectView)
from dataclasses import dataclass,field
import helperFunctions as helper

thisDir = os.path.abspath(os.path.split(__file__)[0])


@dataclass
class projectView:
    projectPath: str = os.getcwd()
    Sites: list = field(default_factory=list)
    
    changeLog: dict = field(init=False)

    # This object is intended to contain the current state the state of database
    # file: str = None
    # timestamp: str = None
    # tableName: str = None
    # newView: bool = False
    # subTable: dict = field(default_factory=dict)
    # currentMetadata: dict = field(default_factory=dict)
    # fileList: dict = field(default_factory=dict)
    # allMetadata: dict = field(default_factory=dict)
    # changeLog: dict = field(default_factory=dict)
    

class myProject():
    def __init__(self,projectPath,defaultArgs={'siteID':None},**kwargs):
        # Apply defaults where not defined if needed, add them as class attributes
        if defaultArgs is not None:
            kwargs = defaultArgs | kwargs
            for k, v in kwargs.items():
                if type(defaultArgs[k])==list and (type(v) != list or len(v) == 1):
                    if type(v) == list: v = v[0]
                    defaultArgs[k][0] = v
                    v = defaultArgs[k]
                elif type(defaultArgs[k])==list and len(defaultArgs[k])>1 and (len(v)%len(defaultArgs[k])) != 0:
                    sys.exit(f'List arg must be of a length divisible by {len(defaultArgs[k])}')
                setattr(self, k, v)

        self.global_config = {}
        with open(f'{thisDir}/config_files/fileFormatsStandard.yml') as yml:
            self.global_config['fileFormats'] = yaml.safe_load(yml)
        self.Path = {}
        self.Path['project'] = os.path.abspath(projectPath)
        self.isProject = os.path.isfile(f"{self.Path['project']}/projectConfig.yml")
        if self.isProject:
            self.readProject()
        else:
            with open(f'{thisDir}/config_files/projectConfigTemplate.yml') as yml:
                self.projectConfig = yaml.safe_load(yml)
        if not self.__class__ is self and not self.isProject and os.path.exists(self.Path['project']):
            sys.exit(f"{self.Path['project']} exists, but it is either corrupted or is not to be a project folder")
        if not self.__class__ is self and not self.__class__ is makeProject and not os.path.exists(self.Path['project']):
            sys.exit(f'Project path does not exist')

    def readProject(self):
        def loadYAML(file):
            with open(file) as yml:
                out = yaml.safe_load(yml)
            return(out)
        # Read the config file & inventory
        self.projectConfig = loadYAML(os.path.join(self.Path['project'],'projectConfig.yml'))
        for key in self.projectConfig['Database'].keys():
            self.Path[key] = os.path.join(self.Path['project'],key)
        self.projectView = {}
        for site in self.projectConfig['Sites']:
            if self.siteID is None or site == self.siteID or site in self.siteID:
                # Load the fileList if for any site
                self.projectView[site] = projectView(
                    fileList=loadYAML(os.path.join(self.Path['rawData'],site,'fileList.json')),
                    changeLog=loadYAML(os.path.join(self.Path['rawData'],site,'changeLog.yml')),
                )
                
    def compareDicts(self,incoming,current,currentName):
        def exclude_callback(obj, path):
            # Exclude any dictionary where it contains a key:value pair 'ignore': True
            if isinstance(obj, dict) and obj.get('ignore') is True:
                return True
            return False
        # def nupdate(mainD,dIn):
        #     for key,value in dIn.items():
        #         if key not in mainD:
        #             mainD[key] = value
        #         else:
        #             mainD[key] = nupdate(mainD[key],value)
        #     return(mainD)
        ignore_Metadata = [r"root.*\['Timestamp']\.*",
            r"root.*\['Source']\.*",
            r"root.*\['Timestamp']\.*",
            r"root.*\['canopy_height']\.*",
            r"root.*\['altitude']\.*",
            r"root.*\['latitude']\.*",
            r"root.*\['longitude']\.*",
            r"root.*\_tube_flowrate']\.*",
            r"root.*\_timelag']\.*",
            ]
        dd = deepdiff.DeepDiff(current,incoming,ignore_order=True,exclude_regex_paths=ignore_Metadata,exclude_obj_callback=exclude_callback)
        if dd == {}:
            return (False)
        dDict = {'ComparedWith':currentName}
        for key,diff in dd.items():
            if key == 'values_changed':
                dDict[key] = {}
                for root,data in diff.items():
                    dpth = root.replace("root['",'').rstrip("']").split("']['")
                    d = {}
                    for i,dp in enumerate(dpth[::-1]):
                        if i == 0:
                            data['acceptNew'] = True
                            d = {dp:data}
                        else:
                            d = {dp:d}
                    dDict[key] = helper.updateDict(dDict[key],d)
            elif key == 'dictionary_item_added':
                d = {}
                for item in diff:
                    dpth = item.replace("root['",'').rstrip("']").replace("']['",'.')
                    d[dpth] = helper.find(dpth,incoming)
                dDict[key] = helper.repackDict(d,format='Nest')
            elif key == 'dictionary_item_removed':
                d = {}
                for item in diff:
                    dpth = item.replace("root['",'').rstrip("']").replace("']['",'.')
                    d[dpth] = helper.find(dpth,current)
                dDict[key] = helper.repackDict(d,format='Nest')
        return dDict
            
    def save(self):
        with open(f"{self.Path['project']}/projectConfig.yml",'+w') as f:
            yaml.dump(self.projectConfig, f, sort_keys=False)

class makeProject(myProject):
    def __init__(self,projectPath,safeMode=True):
        super().__init__(projectPath)
        if self.isProject and safeMode:
            sys.exit(f"{self.Path['project']} exists, specify new directory or rerun with safeMode=False")
        elif self.isProject:
            print('Warning: will delete contents of and existing project in:',self.Path['project'])
            if input('Proceed? Yes/No').upper()=='YES':shutil.rmtree(self.Path['project'])
            else:sys.exit()
        else:os.makedirs(self.Path['project'])
        self.projectPathSetup()
    
    def projectPathSetup(self):
        readme = f'# Readme\n\nCreated by {getpass.getuser()}\non {datetime.datetime.now()}\n'
        for key in self.projectConfig['Database'].keys():
            os.makedirs(f"{self.Path['project']}/{key}")
            txt = self.projectConfig['Database'][key].pop('Purpose')
            readme+=f'\n\n## {key}\n\n* {txt}'
        with open(f"{self.Path['project']}/README.md",'+w') as f:
            f.write(readme)
        with open(f"{self.Path['project']}/projectConfig.yml",'+w') as f:
            yaml.dump(self.projectConfig, f, sort_keys=False)

class importRawData(myProject):
    def __init__(self,projectPath,**kwargs):
        defaultArgs = {
            # inputPath can be string e.g., C:/Datadump
            # Or list of 2xn list of form [root,subdir] e.g, [C:/Datadump,20240731,C:/Datadump,20240831]
            # Will map subdirectory structure to rawData folder, unless inputPath *is* raw data folder
            # Then will assume data were copied manually and will set mode to map
            'siteID':None,
            'debug':False,
            'inputPath':[None,None],
            'mode':'copy',#options: copy (copy files to), move (move files to), map (document existing files and create metadata without moving)
            'fileType':[None],#optoinal: specify specific type(s) or search for all supported types
            'searchTag':[None],#optional: string pattern(s) in filenames **required** for import
            'excludeTag':[None],#optional: string pattern(s) in filenames to **prevent** import
            }
        super().__init__(projectPath,defaultArgs,**kwargs)
        if not self.isProject:
            makeProject(self.Path['project'],self.safeMode)
            self.readProject()
        
        if self.inputPath[0] is None:
            sys.exit('Provide inputPath to continue')
        elif type(self.inputPath[0]) is str and self.inputPath[0].startswith(self.Path['project']):
            self.mode = 'map'
        if self.siteID not in self.projectView.keys():
            self.projectView[self.siteID] = projectView()
        # unpack archive to get record of previous imports
        groups = helper.unpackDict(self.projectView[self.siteID].fileList).values()
        self.fileList = [file[0] for list in groups for file in list]
        for root,subdir in np.array(self.inputPath).reshape(-1,2):
            self.root = os.path.abspath(root)
            if subdir is None: self.subdir = ''
            else: self.subdir = subdir
            inputPath = os.path.join(self.root,self.subdir)
            self.fileTree = {}
            if not os.path.isfile(inputPath) and not os.path.isdir(inputPath):
                sys.exit('Invalid inputPath, must be existing file or directory')
            elif os.path.isdir(inputPath):
                self.filter()
            else:
                self.root,f = os.path.split(inputPath)
                self.importList = [inputPath]
            self.getMetadata()
        self.exportData()
        self.save()

    def filter(self):
        self.importList = []
        if self.searchTag == [None]:
            self.searchTag = []
        if self.excludeTag == [None]:
            self.excludeTag = []
        for curDir, _, fileName in os.walk(self.root):
            if self.subdir in curDir:
                self.importList= self.importList + [os.path.join(curDir,f) for f in fileName 
                                    if (sum(t in f for t in self.excludeTag) == 0 and 
                                        sum(t in f for t in self.searchTag) == len(self.searchTag) and
                                        os.path.join(self.subdir,f) not in self.fileList and
                                        # f not in self.fileList and
                                        f.rsplit('.',1)[1] in self.global_config['fileFormats']['supportedTypes'])]

    def getMetadata(self):                
        # Which parser to use is defined by the file extension
        parsers = {'ghg':[parseGHG.parseGHG()],
                   'dat':[parseTOBA.parseTOBA(),parseMixedArray.parseMixedArray()],
                   'csv':[parseHobo.parseHoboCSV()]
                           }
        cv = self.projectView[self.siteID]
        for file in self.importList:
            for parser in parsers[file.rsplit('.')[-1]]:
                parser.parse(file,mode=1)
                if parser.mode:
                    fn = file.lstrip(self.root+os.sep)
                    # fn = os.path.split(file)[-1]
                    Metadata = parser.Metadata.copy()
                    Metadata['fileContents'] = parser.Contents
                    def repForbid(txt):
                        if txt is None:
                            txt = str(txt)
                        else:
                            for rep in [' ','/','\\']:
                                txt = txt.replace(rep,'_')
                        return(txt)
                    ID = '~'.join([repForbid(Metadata[key]) for key in Metadata.keys() if key != 'fileContents'])
                    
                    print(ID,fn,Metadata)
                    cv.setView(fn,Metadata)
                    if not cv.newView:
                        comp = self.compareDicts(Metadata,
                                       cv.currentMetadata[cv.tableName],
                                       cv.subTable[cv.tableName])
                        if comp:
                            for name in cv.allMetadata[cv.tableName].keys():
                                if name != cv.subTable[cv.tableName]:
                                    comp2 = self.compareDicts(Metadata,
                                                    cv.allMetadata[cv.tableName][name],
                                                    cv.subTable[cv.tableName])
                                    if not comp2:
                                        cv.subTable[cv.tableName] = name
                                        break
                                else:
                                    comp2 = True
                            if comp2:
                                cv.setView(None,Metadata,comp)
                    break
            # try:
            #     print(cv.fileList['TOB3~Flux_Data'])
            # except:
            #     pass

    def exportData(self):
        self.Path['site'] = os.path.join(self.Path['rawData'],self.siteID)
        if self.siteID not in self.projectConfig['Sites']:
            self.projectConfig['Sites'].append(self.siteID)
        if not os.path.isdir(self.Path['site']):
            os.makedirs(self.Path['site'])
        with open(os.path.join(self.Path['site'],'fileList.json'),'w+') as f:
            json.dump(self.projectView[self.siteID].fileList,f)
        with open(os.path.join(self.Path['site'],'changeLog.yml'),'w+') as f:
            yaml.dump(self.projectView[self.siteID].changeLog,f,sort_keys=False, width=1000)
        for table,metadata in self.projectView[self.siteID].allMetadata.items():
            tablePath = os.path.join(self.Path['site'],table)
            if not os.path.isdir(tablePath):
                os.makedirs(tablePath)
            with open(os.path.join(tablePath,'metadata.yml'),'w+') as f:
                yaml.dump(metadata,f,sort_keys=False, width=1000)
            
            for md,value in self.projectView[self.siteID].fileList[table].items():
                for item in value:
                    file = item[0]
                    if self.mode != 'map':
                        dest = os.path.join(tablePath,os.path.split(file)[0])
                        # print(file,dest)
                        if not os.path.isdir(dest):
                            os.makedirs(dest)
                        helper.pasteWithSubprocess(
                            os.path.join(self.root,file),
                            dest,
                            self.mode)
        
class syncMetadata(myProject):
    def __init__(self, projectPath, defaultArgs={ 'siteID': None, 'Verbose':False}, **kwargs):
        super().__init__(projectPath, defaultArgs, **kwargs)
        if self.siteID:
            rd = self.projectConfig['Database']['rawData'][self.siteID] = {}
            # Dump the first metadata file for each table
            for table,view in self.projectView[self.siteID].changeLog.items():
                with open(os.path.join(self.Path['rawData'],self.siteID,table,'metadata.yml')) as f:
                    out = yaml.safe_load(f)
                if table not in self.projectView[self.siteID].currentMetadata.keys():
                    self.projectView[self.siteID].currentMetadata[table] = out[list(out.keys())[0]]
                self.projectView[self.siteID].tableName = table
                rd[table] = self.firstStage(self.projectView[self.siteID].currentMetadata[table])
                rd[table] = self.mdAdjust(rd[table],view,table)
    
    def firstStage(self,md):
        processing = {}
        template = self.projectConfig['Database']['configFiles']['firstStage']['defaultVariable']
        for name,item in md['fileContents'].items():
            processing[name] = copy.deepcopy(template)
            if md['Type'] != 'MixedArray':
                for key in template.keys():
                    if key == 'date_range':
                        processing[name][key] = [md['Timestamp']]
                    elif key in item.keys():
                        processing[name][key] = [item[key]]
                    else:
                        processing[name][key] = [template[key]]
            else:
                for ar,it in item.items():
                    for key in template.keys():
                        if key == 'date_range':
                            processing[name][key] = [md['Timestamp']]
                        elif key in item.keys():
                            processing[name][key] = [it[key]]
                        else:
                            processing[name][key] = [template[key]]
        return(processing)

    def mdAdjust(self,rd,log,table):
        for incoming,comp in log.items():
            if comp is None:
                pass
            else:
                print(comp)
        return(rd)

                # removed = []
                # if len(view.keys())>1:
                #     pairs = list(itertools.combinations(list(view.keys()),2))
                #     for pair in pairs:
                #         pair = list(pair)
                #         if pair[0] not in removed and pair[1] not in removed:
                #             pair.sort()
                #             current = self.projectView[self.siteID].allMetadata[table][pair[0]]
                #             incoming = self.projectView[self.siteID].allMetadata[table][pair[1]]
                #             if not self.compareDicts(current,incoming,pair[1]):
                #                 # remove any duplicates and update fileList correspondingly
                #                 self.projectView[self.siteID].allMetadata[table].pop(pair[1])
                #                 self.projectView[self.siteID].changeLog[table].pop(pair[1])
                #                 tmp = self.projectView[self.siteID].fileList[table].pop(pair[1])
                #                 tmp2 = self.projectView[self.siteID].fileList[table][pair[0]]
                #                 self.projectView[self.siteID].fileList[table][pair[0]] = tmp2+tmp
                #                 self.projectView[self.siteID].fileList[table][pair[0]].sort()
                #                 if self.Verbose: print('Removed duplicate: ',pair[1])
                #                 removed.append(pair[1])

    def mdCorrect(self,log,table):
        for incoming,comp in log.items():
            if comp is None:
                pass
            else:
                if 'values_changed' in comp.keys():
                    # currentMetadata
                    # self.base = helper.unpackDict(self.projectView[self.siteID].allMetadata[table][incoming],format='Nest')
                    self.base = helper.unpackDict(self.projectView[self.siteID].allMetadata[table],format='Nest')
                    self.ComparedWith = comp['ComparedWith']
                    if self.Verbose:print('comp: ',self.ComparedWith)
                    values_changed = comp['values_changed']
                    unpk = helper.unpackDict(values_changed,format='Nest')
                    self.makeChange(unpk)
                    self.projectView[self.siteID].allMetadata[table][incoming] = helper.repackDict(self.base,format='Nest')
                if 'dictionary_item_added' in comp.keys():
                    Adds = comp['dictionary_item_added']
                    print('dictionary_item_added')
                    print(Adds)
                if 'dictionary_item_removed' in comp.keys():
                    Rems = comp['dictionary_item_removed']
                    print('dictionary_item_removed')
                    print(Rems)


    def makeChange(self,changes):
        keys = list(changes.keys())
        self.chg = False
        for new,old,accept in zip(*[iter(keys)]*3):
            if not changes[accept]:
                # Overwrite "erroneous" change picked up with autodetection with old value
                self.chg = True
                if self.Verbose: print('overwriting ',old.rsplit('.',1)[0], ' of ',changes[old],' with ',changes[old])
                self.base[old.rsplit('.',1)[0]] = changes[old]
            elif self.ComparedWith == 'self':
                # Overwriting with user defined values where no change detected
                self.chg = True
                if self.Verbose: print('overwriting ',new.rsplit('.',1)[0], ' of ',self.base[new.rsplit('.',1)[0]],' with ',changes[new])
                self.base[new.rsplit('.',1)[0]] = changes[new]
            elif self.base[new.rsplit('.',1)[0]] != changes[new]:
                # Overwriting with user defined values which diverge from an autodetected change
                chg = True
                self.base[new.rsplit('.',1)[0]] = changes[new]
                if self.Verbose: print('overwriting ',new.rsplit('.',1)[0],' with ',changes[new])
            else:
                if self.Verbose: print('Accepting auto-detected changes in ',old.rsplit('.',1)[0], ' from ',changes[old],' to ',changes[new])

