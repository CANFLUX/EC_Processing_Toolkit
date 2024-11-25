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
import importlib
from parseTables import parseTOBA, parseHobo,parseMixedArray
from parseGHG import parseGHG
importlib.reload(parseTOBA)
importlib.reload(parseHobo)
importlib.reload(parseMixedArray)
importlib.reload(parseGHG)
import helperFunctions as helper


thisDir = os.path.abspath(os.path.split(__file__)[0])

class myProject():
    def __init__(self,projectPath):
        self.global_config = {}
        with open(f'{thisDir}/config_files/fileFormatsStandard.yml') as yml:
            self.global_config['fileFormats'] = yaml.safe_load(yml)
        self.projectPath = os.path.abspath(projectPath)
        self.rawPath = os.path.abspath(f"{self.projectPath}/rawData")
        self.isProject = os.path.isfile(f'{self.projectPath}/projectConfig.yml')
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
        # Read the config file
        with open(f'{self.projectPath}/projectConfig.yml') as yml:
            self.projectConfig = yaml.safe_load(yml)
        # Read the Inventory
        fileInventory = os.path.join(self.rawPath,'fileInventory.json')
        if os.path.isfile(fileInventory):
            with open(fileInventory) as f:
                self.fileInventory = json.load(f)
        else:
            self.fileInventory = {'Processed':{},
                                  'Pending':{},
                                  'Failures':{}}
            
    def unpackPath(self,fileTree):
        # recursive function to unpack fileTree dict
        def unpack(child,parent=None,root=None):
            pth = {}
            if type(child) is dict:
                for key,value in child.items():
                    if parent is None:
                        pass
                    else:
                        key = os.path.join(parent,key)
                    if type(value) is list:
                        pth[key] = unpack(value,key,root)
                    else:
                        pth = pth | unpack(value,key,root)

            else:
                if type(child)==list:
                    return(child)
                else:
                    sys.exit('Error in file tree unpack')
            return(pth)
        return(unpack(fileTree))
    
    def repackPath(self,fileList,root=None):
        if root == None:
            root = self.root
        fileTree = {}
        fileTree[root] = {}
        for file,info in fileList.items():
            d,f = os.path.split(file)
            d = d.lstrip(root).lstrip(os.sep) 
            if d not in fileTree[root].keys():
                fileTree[root][d] = {}
            fileTree[root][d][f] = info
        return(fileTree)
            
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
        for key,value in self.projectConfig['Directories'].items():
            os.makedirs(f"{self.projectPath}/{key}")
            readme+=f'\n\n## {key}\n\n* {value["Purpose"]}'
        with open(f"{self.projectPath}/README.md",'+w') as f:
            f.write(readme)
        with open(f"{self.projectPath}/projectConfig.yml",'+w') as f:
            yaml.dump(self.projectConfig, f, sort_keys=False)


class importRawData(myProject):
    def __init__(self,projectPath,**kwargs):
        super().__init__(projectPath)
        defaultArgs = {
            # inputPath can be string e.g., C:/Datadump
            # Or list of 2xn list of form [root,subdir] e.g, [C:/Datadump,20240731,C:/Datadump,20240831]
            # Will map subdirectory structure to rawData folder, unless inputPath *is* raw data folder
            # Then will assume data were copied manually and will set mode to map
            'siteID':None,
            'inputPath':[None,None],
            'mode':'copy',#options: copy (copy files to), move (move files to), map (document existing files and create metadata without moving)
            'fileType':[None],#optoinal: specify specific type(s) or search for all supported types
            'searchTag':[],#optional: string pattern(s) in filenames **required** for import
            'excludeTag':[],#optional: string pattern(s) in filenames to **prevent** import
            }
        # Apply defaults where not defined
        kwargs = defaultArgs | kwargs
        # add arguments as class attributes
        for k, v in kwargs.items():
            print(k,v)
            print(defaultArgs)
            if type(defaultArgs[k])==list and (type(v) != list or len(v) == 1):
                if type(v) == list: v = v[0]
                defaultArgs[k][0] = v
                v = defaultArgs[k]
            elif type(defaultArgs[k])==list and len(defaultArgs[k])>1 and (len(v)%len(defaultArgs[k])) != 0:
                sys.exit('Input path must be string or 2xn list of form [root,subdir, ...] ')
            setattr(self, k, v)
        if not self.isProject:
            makeProject(self.projectPath,self.safeMode)
            self.readProject()
        self.supportedTypes = {v:k for k,val in self.global_config['fileFormats']['supportedTypes'].items() for v in val}
        if self.fileType[0] is not None:
            if any([f in self.supportedTypes.keys() for f in self.fileType]):
                self.supportedTypes = {k:self.supportedTypes[k] for k in self.fileType}
            else:
                sys.exit('Specify valid file type, or exclude for autodetection')
        self.extensions = self.supportedTypes.values()
        if self.inputPath[0] is None:
            sys.exit('Provide inputPath to continue')
        elif type(self.inputPath[0]) is str and self.inputPath[0].startswith(self.projectPath):
            self.mode = 'map'
        self.fileList = list(self.unpackPath(self.fileInventory['Pending']).keys()) + list(self.unpackPath(self.fileInventory['Processed']).keys())
        for self.root,self.subdir in np.array(self.inputPath).reshape(-1,2):
            self.root = os.path.abspath(self.root)
            if self.subdir is None: self.subdir = ''
            inputPath = os.path.join(self.root,self.subdir)
            self.fileTree = {os.path.abspath(self.root):{}}
            if not os.path.isfile(inputPath) and not os.path.isdir(inputPath):
                sys.exit('Invalid inputPath, must be existing file or directory')
            elif os.path.isdir(inputPath):
                self.filter()
            else:
                self.root,f = os.path.split(inputPath)
                self.fileTree[os.path.abspath(self.root)] = {f:[False,None]}
            self.Metadata = {'Files':[],
                            'Current':{},
                            'ID':{}}
            self.getMetadata()
        with open(os.path.join(self.rawPath,'fileInventory.json'),'w+') as f:
            json.dump(self.fileInventory,f)

    def filter(self):
        for curDir, _, fileName in os.walk(self.root):
            if self.subdir in curDir:
                self.fileTree[self.root][os.path.relpath(curDir, self.root)] = {f:[False,None] 
                    for f in fileName if (sum(t in f for t in self.excludeTag) == 0 and 
                        sum(t in f for t in self.searchTag) == len(self.searchTag) and
                        os.path.join(curDir,f) not in self.fileList and
                        f.rsplit('.',1)[1] in self.extensions)}

    def getMetadata(self):
        self.fileTree = self.unpackPath(self.fileTree)
        for file in self.fileTree.keys():
            attempt = 0
            if file.endswith('dat'):
                pT = parseTOBA.parseTOBA()
                pT.parse(file,mode=1)
                attempt += pT.mode
                if pT.mode:
                    self.exportData(pT.Metadata,file)
                else:
                    pMA = parseMixedArray.parseMixedArray()
                    pMA.parse(file,mode=1)
                    if pMA.mode:
                        self.exportData(pMA.Metadata,file)
                    attempt += pMA.mode
            elif file.endswith('ghg'):
                pGHG = parseGHG.parseGHG()
                pGHG.parse(file,mode=1)
                attempt += pH.mode
                if pGHG.mode:
                    self.exportData(pGHG.Metadata,file)
            elif file.endswith('csv'):
                pH = parseHobo.parseHoboCSV()
                pH.parse(file,mode=1)
                attempt += pH.mode
                if pH.mode:
                    self.exportData(pH.Metadata,file)
                else:
                    print(f'add case for {file.rsplit(".",1)[-1]}')
        # self.exportData()
        failures = {key:value for key,value in self.fileTree.items() if value[1] is None}
        for f in failures.keys():
            self.fileTree.pop(f)
        if self.root not in self.fileInventory['Pending'].keys():
            self.fileInventory['Pending'][self.root] = {}
            self.fileInventory['Failures'][self.root] = {}
        self.fileInventory['Pending'][self.root] = self.fileInventory['Pending'][self.root] | self.repackPath(self.fileTree)[self.root]
        self.fileInventory['Failures'][self.root] = self.fileInventory['Failures'][self.root] | self.repackPath(failures)[self.root]
            
        
    def exportData(self,Metadata,file):
        # if len(self.Metadata['Files']):
            subDir = os.path.relpath(os.path.split(file)[0],self.root)
            mdName = os.path.split(file)[-1].rsplit('.',1)[0]+f"_Metadata.yml"
            self.dest = os.path.join(self.rawPath,'' if self.siteID is None else self.siteID,f"{Metadata['Table']}",subDir)
            if not os.path.isdir(self.dest):
                os.makedirs(self.dest)   
            with open(os.path.join(self.dest,mdName),'w+') as f:
                yaml.dump(Metadata, f, sort_keys=False)
                # yaml.dump({'Imported':self.Metadata['Files']}, f, sort_keys=False)
            
            if self.mode != 'map':
                # pb = helper.progressbar(len(self.Metadata['Files']))
                # for f in self.Metadata['Files']:
                self.fileTree[file][1] = mdName
                helper.pasteWithSubprocess(file,self.dest,self.mode)
                #     pb.step()
                # pb.close()
        
class syncMetadata(myProject):
    def __init__(self,projectPath,**kwargs):
        super().__init__(projectPath)
        defaultArgs = {
            'siteID':None,}
        # Apply defaults where not defined
        kwargs = defaultArgs | kwargs
        # add arguments as class attributes
        for k, v in kwargs.items():
            if type(defaultArgs[k])==list and (type(v) != list or len(v) == 1):
                if type(v) == list: v = v[0]
                defaultArgs[k][0] = v
                v = defaultArgs[k]
            elif type(defaultArgs[k])==list and len(defaultArgs[k])>1 and (len(v)%len(defaultArgs[k])) != 0:
                sys.exit('Input path must be string or 2xn list of form [root,subdir, ...] ')
            setattr(self, k, v)
        if self.siteID is not None:
            self.rawPath = os.path.join(self.rawPath,self.siteID)

        self.firstStage = {}
        self.representativeFiles = {}
        for d in os.listdir(self.rawPath):
            self.representativeFiles[d] = {}
            self.firstStage[d] = {}
            mdFiles = []
            for dpath,_,files in os.walk(os.path.join(self.rawPath,d)):
                mdFiles = mdFiles + [os.path.join(dpath,f) for f in files if f.endswith('Metadata.yml')]
            self.current = {}
            for file in mdFiles:
                with open(file,'r',encoding='utf-8') as f:
                    if self.current == {}:
                        self.setCurrent(yaml.safe_load(f))
                        self.representativeFiles[d] = [file.lstrip(self.rawPath)]
                    else:
                        change = self.compareMetadata(yaml.safe_load(f))
                        if change:
                            print(change)
                            self.representativeFiles[d].append(file.lstrip(self.rawPath))
                    
    def setCurrent(self,new):
        self.current = new
        if 'Timestamp' in self.current.keys():
            self.startTime = self.current['Timestamp']
            self.current.pop('Timestamp')
    
    def compareMetadata(self,incoming):
        change = False
        if 'Timestamp' in incoming.keys():
            timestamp = incoming['Timestamp']
            incoming.pop('Timestamp')
        if incoming == self.current:
            return(change)
        else:
            for key in self.current.keys():
                if self.current[key] != incoming[key]:
                    change = key
                    break
            if change == 'Header':
                a = set(self.current['Header'])
                b = set(incoming['Header'])
                if len(a ^ b)==0:
                    change = change+'_subHeaderOnly'
                else:
                    if set (set(a)- set(b)) and set (set(b)- set(a)):
                        change = change+'_Add_and_Drop'
                    elif set (set(a)- set(b)):
                        change = change+'_Drop'
                    elif set (set(b)- set(a)):
                        change = change+'_Add'
                    else:
                        print('unknown exception')
            return(change)