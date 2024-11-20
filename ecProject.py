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
from io import StringIO
from pathlib import Path
from parseTables.parseTOBA import parseTO
from parseTables.parseHobo import parseHoboCSV
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
            self.fileInventory = {'Pending':{},
                                  'Processed':{}}
            
    def unpackPath(self,fileSet):
        # recursive function to unpack fileTree dict
        def unpack(child,parent=None,root=None):
            pth = []
            if type(child) is dict:
                for key,value in child.items():
                    if parent is None:
                        pass
                    else:
                        key = os.path.join(parent,key)
                    if value!= []:
                        pth = pth + unpack(value,key,root)
            else:
                if type(child[0])==list:
                    return([os.path.join(parent,c[0]) for c in child])
                else:
                    return([os.path.join(parent,c) for c in child])
            return(pth)
        return(unpack(fileSet))
            
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
        for key,value in self.projectConfig.items():
            os.makedirs(f"{self.projectPath}/{key}")
            readme+=f'\n\n## {key}\n\n* {value["Purpose"]}'
        with open(f"{self.projectPath}/README.md",'+w') as f:
            f.write(readme)
        with open(f"{self.projectPath}/projectConfig.yml",'+w') as f:
            yaml.dump(self.projectConfig, f, sort_keys=False)

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
class importData(myProject):
    def __init__(self,projectPath,**kwargs):
        super().__init__(projectPath)
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
        self.fileList = self.unpackPath(self.fileInventory['Pending']) + self.unpackPath(self.fileInventory['Processed'])
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
                self.fileTree[os.path.abspath(self.root)] = [[f,False,None]]
        
            self.Metadata = {'Files':[],
                            'Current':{},
                            'ID':{}}
            self.getMetadata(self.fileTree)
            if self.root in self.fileInventory['Pending'].keys():
                for key,value in self.fileTree[self.root].items():
                    if key not in self.fileInventory['Pending'][self.root].keys():
                        self.fileInventory['Pending'][self.root][key] = value
                    else:
                        self.fileInventory['Pending'][self.root][key] = self.fileInventory['Pending'][self.root][key] + value
            else:
                self.fileInventory['Pending'][self.root] = self.fileTree[self.root]
        with open(os.path.join(self.rawPath,'fileInventory.json'),'w+') as f:
            json.dump(self.fileInventory,f)

    def filter(self):
        for curDir, _, fileName in os.walk(self.root):
            if self.subdir in curDir:
                self.fileTree[self.root][os.path.relpath(curDir, self.root)] = [[f,False,None] 
                    for f in fileName if (sum(t in f for t in self.excludeTag) == 0 and 
                        sum(t in f for t in self.searchTag) == len(self.searchTag) and
                        os.path.join(curDir,f) not in self.fileList and
                        f.rsplit('.',1)[1] in self.extensions)]

    def getMetadata(self,fileSet):
        fileList = self.unpackPath(fileSet)
        for file in fileList:
            if file.endswith('dat'):
                pT = parseTO()
                pT.parse(file,mode=0)
                if pT.mode >= 0:
                    self.compareMetadata(pT.Metadata,file)
                else:
                    print(f'add case for {file.rsplit(".",1)[-1]}')
            elif file.endswith('csv'):
                pH = parseHoboCSV()
                pH.parse(file,mode=0)
                if pH.mode >= 0:
                    self.compareMetadata(pH.Metadata,file)
                else:
                    print(f'add case for {file.rsplit(".",1)[-1]}')
        self.exportData()
            
    def compareMetadata(self,incoming,fpath):
        if 'Timestamp' in incoming.keys():
            Timestamp = incoming['Timestamp']
            incoming.pop('Timestamp')      
        if self.Metadata['Current'] == {}:
            self.Metadata['Current'] = incoming
            self.Metadata['Change'] = True
        elif self.Metadata['Current'] == incoming:
            self.Metadata['Change'] = False
        else:
            for key in self.Metadata['Current'].keys():
                if self.Metadata['Current'][key] != incoming[key]:
                    self.Metadata['Change'] = f'New{key}'
                    break
            if self.Metadata['Change'] == 'NewColumnHeaders':
                a = set(self.Metadata['Current']['ColumnHeaders'])
                b = set(incoming['ColumnHeaders'])
                Change = self.Metadata['Change']
                if len(a ^ b)==0:
                    Change = Change+'_subHeaderOnly'
                else:
                    if set (set(a)- set(b)) and set (set(b)- set(a)):
                        Change = Change+'_Add_and_Drop'
                    elif set (set(a)- set(b)):
                        Change = Change+'_Drop'
                    elif set (set(a)- set(b)) and set (set(b)- set(a)):
                        Change = Change+'_Add'
                self.Metadata['Change'] = Change
        if self.Metadata['Change']:
            # Write "current" and move files before setting new
            if self.Metadata['ID']!={}:
                self.exportData()
            if incoming['Table'] not in self.Metadata['ID'].keys():
                self.Metadata['ID'] = {incoming['Table']:0}
            else:
                self.Metadata['ID'][incoming['Table']]+=1
            self.MetadataID = self.Metadata['ID'][incoming['Table']]
            self.Metadata['Current'] = incoming
            self.Metadata['Files'] = [fpath]
        else:
            self.Metadata['Files'].append(fpath)
        
    def exportData(self):
        if len(self.Metadata['Files']):
            subDir = os.path.relpath(os.path.split(self.Metadata['Files'][0])[0],self.root)
            self.dest = os.path.join(
                self.rawPath,
                '' if self.siteID is None else self.siteID,
                subDir,
                f"{self.Metadata['Current']['Table']}_{self.MetadataID}"
                )
            if not os.path.isdir(self.dest):
                os.makedirs(self.dest)            
            with open(os.path.join(self.dest,'Metadata.yml'),'w+') as f:
                yaml.dump(self.Metadata['Current'], f, sort_keys=False)
                yaml.dump({'Imported':self.Metadata['Files']}, f, sort_keys=False)
            
            if self.mode != 'map':
                pb = helper.progressbar(len(self.Metadata['Files']))
                for f in self.Metadata['Files']:
                    helper.pasteWithSubprocess(f,self.dest,self.mode)
                    pb.step()
                pb.close()
        