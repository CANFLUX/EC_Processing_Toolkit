import os
import re
import sys
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
import helperFunctions as helper

thisDir = os.path.abspath(os.path.split(__file__)[0])

class myProject():
    def __init__(self,projectPath):
        with open(f'{thisDir}/config_files/standardFileFormats.yml') as yml:
            self.standardInputFileInfo = yaml.safe_load(yml)
        self.projectPath = os.path.abspath(projectPath)
        self.rawPath = os.path.abspath(f"{self.projectPath}/rawSiteData")
        self.isProject = os.path.isfile(f'{self.projectPath}/projectConfig.yml')
        if self.isProject:
            self.readProject()
        else:
            with open(f'{thisDir}/config_files/projectConfig.yml') as yml:
                self.projectConfig = yaml.safe_load(yml)
        if not self.__class__ is self and not self.isProject and os.path.exists(self.projectPath):
            sys.exit(f'{self.projectPath} exists, but it is either corrupted or is not to be a project folder')
        if not self.__class__ is self and not self.__class__ is makeProject and not os.path.exists(self.projectPath):
            sys.exit(f'Project path does not exist')

    def readProject(self):
        # Read the config file
        with open(f'{self.projectPath}/projectConfig.yml') as yml:
            self.projectConfig = yaml.safe_load(yml)
        # Read the raw inventory
        # self.rawSiteDataInventory = pd.read_csv(f"{self.rawPath}/fileInventory.csv")
        # # Find any raw data folder that have outstanding imports
        # self.Processed = pd.DataFrame()
        # for i,row in self.rawSiteDataInventory.loc[self.rawSiteDataInventory['Processed']==False].iterrows():
        #     dpath = os.path.abspath(f"{self.rawPath}/{row['relDir']}/")
        #     tmp = pd.read_csv(f"{dpath}/fileInventory.csv")
        #     tmp.index = [row['relDir'] for i in range(tmp.shape[0])]
        #     if self.Processed.empty:
        #         self.Processed = tmp.copy()
        #     else:
        #         self.Processed = pd.concat([self.Processed,tmp])
        # if not self.Processed.empty:
        #     print('Outstanding imports: ',self.Processed.loc[self.Processed['Processed']==False,'fileName'].count())
            
            
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
            self.projectConfig[key]['inventory']=None
        with open(f"{self.projectPath}/README.md",'+w') as f:
            f.write(readme)
        with open(f"{self.projectPath}/projectConfig.yml",'+w') as f:
            yaml.dump(self.projectConfig, f, sort_keys=False)