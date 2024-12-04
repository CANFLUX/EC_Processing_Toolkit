import os
import re
import sys
import json
import yaml
import time
import shutil
import fnmatch
import getpass
import datetime
import numpy as np
import pandas as pd
from pathlib import Path
import deepdiff
import importlib
from dataclasses import dataclass,field
from parseTables import parseTOBA, parseHobo,parseMixedArray
from parseGHG import parseGHG
importlib.reload(parseTOBA)
importlib.reload(parseHobo)
importlib.reload(parseMixedArray)
importlib.reload(parseGHG)
import helperFunctions as helper


thisDir = os.path.abspath(os.path.split(__file__)[0])


@dataclass
class currentView:
    file: str = None
    table: str = None
    tableName: str = None
    newView: bool = False
    fileList: dict = field(default_factory=dict)
    mdName: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    archive: dict = field(default_factory=dict)
    
    def setView(self,file,Metadata,ddiff=None):
        if self.newView:
            self.newView = False
        elif self.file is not None:
            nm = self.mdName[self.tableName]
            self.archive[self.tableName][nm]['fileList'].append(self.file)
            
        if file is not None:
            self.file = file
            tableName = '_'.join([value for key,value in Metadata.items() 
                    if value is not None and key in 
                    ['Type','StationName','Logger','SerialNo','Table']])
            for rep in [' ','/','\\','__']:
                tableName = tableName.replace(rep,'_')
            self.tableName = tableName
            
        if self.tableName not in self.archive.keys() or ddiff is not None:
            if self.tableName not in self.archive.keys():
                self.archive[self.tableName] = {}
                # self.fileList[self.tableName] = {}
            self.metadata[self.tableName] = Metadata.copy()
            mdName = '_'.join(['_metadata']+[value for key,value in Metadata.items() 
                    if value is not None and key in['Program','Frequency','Timestamp']])+'.yml'
            for rep in [' ','/','\\','__']:
                mdName = mdName.replace(rep,'_')
            if mdName in self.archive[self.tableName].keys():
                print('Duplicate Warning!')
                print('Update Metadata Naming')

            self.mdName[self.tableName] = mdName
            self.archive[self.tableName][mdName] = {}
            self.archive[self.tableName][mdName]['fileList'] = [self.file]
            self.archive[self.tableName][mdName]['Metadata'] = self.metadata[self.tableName]
            self.archive[self.tableName][mdName]['ChangeLog'] = ddiff


class myProject():
    def __init__(self,projectPath,defaultArgs={'siteID':None},**kwargs):
        self.global_config = {}
        with open(f'{thisDir}/config_files/fileFormatsStandard.yml') as yml:
            self.global_config['fileFormats'] = yaml.safe_load(yml)
        self.projectPath = os.path.abspath(projectPath)
        self.rawPath = os.path.abspath(f"{self.projectPath}/rawData")
        self.isProject = os.path.isfile(f'{self.projectPath}/projectConfig.yml')
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
        if self.isProject:
            self.readProject()
        else:
            with open(f'{thisDir}/config_files/projectConfigTemplate.yml') as yml:
                self.projectConfig = yaml.safe_load(yml)
        if not self.__class__ is self and not self.isProject and os.path.exists(self.projectPath):
            sys.exit(f'{self.projectPath} exists, but it is either corrupted or is not to be a project folder')
        if not self.__class__ is self and not self.__class__ is makeProject and not os.path.exists(self.projectPath):
            sys.exit(f'Project path does not exist')

    def readProject(self):
        # Read the config file & inventory
        with open(f'{self.projectPath}/projectConfig.yml') as yml:
            self.projectConfig = yaml.safe_load(yml)
        self.currentView = currentView()
        for site in self.projectConfig['rawData']['Sites']:
            if self.siteID is None or site == self.siteID or site in self.siteID:
                apth = os.path.join(self.projectPath,'rawData',site,'fileList.json')
                with open(apth) as f:
                    self.currentView.fileList[site] = json.load(f)
    def unpackDict(self,Tree,format='Path'):
        # recursive function to unpack fileTree dict
        def unpack(child,parent=None,root=None,format='Path'):
            pth = {}
            if type(child) is dict:
                for key,value in child.items():
                    if parent is None:
                        pass
                    elif format == 'Path':
                        key = os.path.join(parent,key)
                    elif format == 'Nest':
                        key = '.'.join([parent,key])
                    if type(value) is not dict:
                        pth[key] = unpack(value,key,root,format)
                    else:
                        pth = pth | unpack(value,key,root,format)
            else:
                if type(child) is not dict:
                    return(child)
                else:
                    sys.exit('Error in file tree unpack')
            return(pth)
        return(unpack(Tree,format=format))
        
    def repackDict(self,fileList,format='Path'):
        fileTree = {}
        for file,info in fileList.items():
            if format == 'Path':
                b = file.split(os.path.sep)
            elif format == 'Nest':
                b = file.split('.')
            else:
                sys.exit('invalid format')
            for i in range(len(b),0,-1):
                if i == len(b):
                    subTree = {b[i-1]:info}
                else:
                    subTree =  {b[i-1]:subTree}
            fileTree = helper.updateDict(fileTree,subTree)
        return(fileTree)
            
    def save(self):
        with open(f"{self.projectPath}/projectConfig.yml",'+w') as f:
            yaml.dump(self.projectConfig, f, sort_keys=False)

class makeProject(myProject):
    def __init__(self,projectPath,safeMode=True):
        super().__init__(projectPath)
        if self.isProject and safeMode:
            sys.exit(f'{self.projectPath} exists, specify new directory or rerun with safeMode=False')
        elif self.isProject:
            print('Warning: will delete contents of and existing project in:',self.projectPath)
            if input('Proceed? Yes/No').upper()=='YES':shutil.rmtree(self.projectPath)
            else:sys.exit()
        else:os.makedirs(self.projectPath)
        self.projectPathSetup()
    
    def projectPathSetup(self):
        readme = f'# Readme\n\nCreated by {getpass.getuser()}\non {datetime.datetime.now()}\n'
        for key in self.projectConfig.keys():
            os.makedirs(f"{self.projectPath}/{key}")
            txt = self.projectConfig[key].pop('Purpose')
            readme+=f'\n\n## {key}\n\n* {txt}'
        with open(f"{self.projectPath}/README.md",'+w') as f:
            f.write(readme)
        with open(f"{self.projectPath}/projectConfig.yml",'+w') as f:
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
            'searchTag':[],#optional: string pattern(s) in filenames **required** for import
            'excludeTag':[],#optional: string pattern(s) in filenames to **prevent** import
            }
        super().__init__(projectPath,defaultArgs,**kwargs)
        if not self.isProject:
            makeProject(self.projectPath,self.safeMode)
            self.readProject()
        
        if self.inputPath[0] is None:
            sys.exit('Provide inputPath to continue')
        elif type(self.inputPath[0]) is str and self.inputPath[0].startswith(self.projectPath):
            self.mode = 'map'
        # unpack archive to get record of previous imports
        groups = self.unpackDict(self.currentView.fileList).values()
        self.fileList = [file for list in groups for file in list]
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
        for curDir, _, fileName in os.walk(self.root):
            if self.subdir in curDir:
                self.importList= self.importList + [os.path.join(curDir,f) for f in fileName 
                                    if (sum(t in f for t in self.excludeTag) == 0 and 
                                        sum(t in f for t in self.searchTag) == len(self.searchTag) and
                                        os.path.join(self.subdir,f) not in self.fileList and
                                        f.rsplit('.',1)[1] in self.global_config['fileFormats']['supportedTypes'])]

    def getMetadata(self):
        def compare(incoming,current):
            def exclude_callback(obj, path):
                # Exclude any dictionary where 'ignore': True
                if isinstance(obj, dict) and obj.get('ignore') is True:
                    return True
                return False
            def nupdate(mainD,dIn):
                for key,value in dIn.items():
                    if key not in mainD:
                        mainD[key] = value
                    else:
                        mainD[key] = nupdate(mainD[key],value)
                return(mainD)
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
            else:
                dDict = {'ComparedWith':self.currentView.mdName[self.currentView.tableName]}
                for key,diff in dd.items():
                    if key == 'values_changed':
                        dDict[key] = {}
                        for root,data in diff.items():
                            dpth = root.replace("root['",'').rstrip("']").split("']['")#,'.')
                            d = {}
                            for i,dp in enumerate(dpth[::-1]):
                                if i == 0:
                                    data['acceptNew'] = True
                                    d = {dp:data}
                                else:
                                    d = {dp:d}
                            dDict[key] = nupdate(dDict[key],d)
                            # dDict[dpth] = data
                    elif key == 'dictionary_item_added':
                        d = {}
                        for item in diff:
                            dpth = item.replace("root['",'').rstrip("']")
                            d[dpth] = incoming[dpth]
                        dDict[key] = d
                    elif key == 'dictionary_item_removed':
                        d = {}
                        for item in diff:
                            dpth = item.replace("root['",'').rstrip("']")
                            d[dpth] = current[dpth]
                        dDict[key] = d
                return dDict
                
        # Which parser to use is defined by the file extension
        parsers = {'ghg':[parseGHG.parseGHG()],
                   'dat':[parseTOBA.parseTOBA(),parseMixedArray.parseMixedArray()],
                   'csv':[parseHobo.parseHoboCSV()]
                           }
        for file in self.importList:
            for parser in parsers[file.rsplit('.')[-1]]:
                parser.parse(file,mode=1)
                if parser.mode:
                    fn = file.lstrip(self.root+os.sep)
                    Metadata = parser.Metadata.copy()
                    Metadata['fileContents'] = parser.Contents
                    self.currentView.setView(fn,Metadata)
                    if not self.currentView.newView:
                        comp = compare(Metadata,self.currentView.metadata[self.currentView.tableName])
                        if comp:
                            for name in self.currentView.archive[self.currentView.tableName].keys():
                                if name != self.currentView.mdName[self.currentView.tableName]:
                                    comp2 = compare(Metadata,self.currentView.archive[self.currentView.tableName][name]['Metadata'])
                                    if not comp2:
                                        self.currentView.mdName[self.currentView.tableName] = name
                                        break
                                else:
                                    comp2 = True
                            if comp2:
                                self.currentView.setView(None,Metadata,comp)
                    break
            if not 'parser' in locals():
                print('Did not process',file)

    def exportData(self):
        sitePath = os.path.join(self.rawPath,self.siteID)
        if self.siteID not in self.projectConfig['rawData']['Sites']:
            self.projectConfig['rawData']['Sites'].append(self.siteID)
        if not os.path.isdir(sitePath):
            os.makedirs(sitePath)
            
        with open(os.path.join(sitePath,'fileList.json'),'w+') as f:
            archive = {table:{fn:log['fileList'] for fn,log in value.items() } for table,value in self.currentView.archive.items()} 
            if self.siteID not in self.currentView.fileList.keys():
                self.currentView.fileList[self.siteID] = {}

            for table,data in archive.items():
                if table not in self.currentView.fileList[self.siteID].keys():
                    self.currentView.fileList[self.siteID][table] = {}
                for md,files in data.items():
                    if md not in self.currentView.fileList[self.siteID][table].keys():
                        self.currentView.fileList[self.siteID][table][md] = []
                    for file in files:
                        self.currentView.fileList[self.siteID][table][md].append(file)
            json.dump(self.currentView.fileList[self.siteID],f)
        for table,metadata in self.currentView.archive.items():
            tablePath = os.path.join(sitePath,table)
            if not os.path.isdir(tablePath):
                os.makedirs(tablePath)
            with open(os.path.join(tablePath,'_changeLog.yml'),'w+') as f:
                changeLog = {f:log['ChangeLog'] for f,log in metadata.items()} 
                yaml.dump(changeLog,f,sort_keys=False, width=1000)
            for key,value in metadata.items():
                with open(os.path.join(tablePath,key),'w+') as f:
                    yaml.dump(value['Metadata'],f,sort_keys=False, width=1000)
            for md,value in self.currentView.fileList[self.siteID][table].items():
                for file in value:
                    if self.mode != 'map':
                        dest = os.path.join(tablePath,os.path.split(file)[0])
                        if not os.path.isdir(dest):
                            os.makedirs(dest)
                        helper.pasteWithSubprocess(
                            os.path.join(self.root,file),
                            dest,
                            self.mode)
        