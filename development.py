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
        self.rawSiteDataInventory = pd.read_csv(f"{self.rawPath}/fileInventory.csv")
        # Find any raw data folder that have outstanding imports
        self.Processed = pd.DataFrame()
        for i,row in self.rawSiteDataInventory.loc[self.rawSiteDataInventory['Processed']==False].iterrows():
            dpath = os.path.abspath(f"{self.rawPath}/{row['relDir']}/")
            tmp = pd.read_csv(f"{dpath}/fileInventory.csv")
            tmp.index = [row['relDir'] for i in range(tmp.shape[0])]
            if self.Processed.empty:
                self.Processed = tmp.copy()
            else:
                self.Processed = pd.concat([self.Processed,tmp])
        if not self.Processed.empty:
            print('Outstanding imports: ',self.Processed.loc[self.Processed['Processed']==False,'fileName'].count())
            
            
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
            yaml.dump(self.projectConfig, f)
        pd.DataFrame(data={'relDir':[],'Processed':[]}).to_csv(f"{self.rawPath}/fileInventory.csv",index=False)

def documentFile(inputFile,saveTo,skipRows=0,headerRows=1,sep=',',ignoreByDefault=True):
    saveAs = f"{saveTo}/metaData.yml"
    with open(inputFile,'r') as file:
        for _ in range(skipRows):
            next(file)
        Header = StringIO(''.join([file.readline() for _ in range(headerRows)]))
    Header = pd.read_csv(Header)
    if headerRows > 1:
        unitIn = {col:unit for col,unit in Header.loc[0].items()}
    else:
        unitIn = {col:'' for col in Header.columns}            
    metaData = {'metaData':{
        'createdBy':'',
        'lastEdit':datetime.datetime.now().strftime('%Y-%m-%d'),
        'readyForImport':False
        }}
    metaData['inputFileDescription'] = {col:{'standardName':'',
            'ignore':ignoreByDefault,
            'Period_1':{'unitIn':unitIn[col],
                'unitOut':unitIn[col],
                'recalibrate':'',
                'dateRange':''}} for col in Header.columns}
    with open(saveAs,'w+') as f:
        print('Creating template: ',saveAs,'\n\nEdit this file before proceeding with import\n\n')
        yaml.dump(metaData,f, sort_keys=False)

dataDumpArgs = {
    'siteID':None,
    'dIn':None,
    'subPath':'',
    'byMonth':True,
    'parseDate':True,
    'overWrite':False,
    'fileType':['TOA5','GHG'],
    'metaDataTemplate':None,
    'searchTag':[],
    'excludeTag':[],
    'safeMode':True,
    'mode':'copy'
    }

class dataDump(myProject):
    def __init__(self,projectPath,**kwargs):
        super().__init__(projectPath)
        # Apply defaults where not defined
        kwargs = dataDumpArgs | kwargs
        # add arguments as class attributes
        for k, v in kwargs.items():
            if type(dataDumpArgs[k])==list and type(v) != list:
                v = [v]
            setattr(self, k, v)
        if not self.isProject:
            makeProject(self.projectPath,self.safeMode)
            self.readProject()
        if self.dIn is not None:
            print('Searching ',self.dIn)
            for self.curDir, _, fileName in os.walk(self.dIn):
                for fileType in self.fileType:
                    fileName = [s for s in fileName if  
                                sum(t in s for t in self.excludeTag) == 0 and
                                sum(t in s for t in self.searchTag) == len(self.searchTag) and
                                s.split('.')[-1] in self.standardInputFileInfo[fileType]['extension']]
                    if len(fileName)>0:
                        self.buildInventory(fileName,fileType)
            self.rawSiteDataInventory.to_csv(f"{self.rawPath}/fileInventory.csv",index=False)

    def buildInventory(self,fileName,fileType):
        fileInfo = self.standardInputFileInfo[fileType]
        source = [os.path.abspath(self.curDir+'/'+f) for f in fileName]
        subDir = f"{self.siteID}/{fileType}/{self.subPath}"
        if self.parseDate:
            try:
                srch = [re.search(fileInfo['search'], f.rsplit('.',1)[0]).group(0) for f in fileName]
                Interval = pd.to_datetime([datetime.datetime.strptime(s,fileInfo['format']) for s in srch]) 
                relDir = [f"{subDir}/{M}" for M in Interval.to_period('M').strftime('%Y%m')]
            except:
                self.parseDate = False  
                print('Warning!!\n\nUnable to parse date pattern from input file, proceeding without') 
        if not self.parseDate:
            Interval = [i for i in range(len(fileName))]
            relDir = [subDir for _ in Interval]        
        metadataFile = f"{self.rawPath}/{subDir}/metadata.yml"
        if os.path.isfile(metadataFile):
            with open(metadataFile) as f:
                self.metaData = yaml.safe_load(f)
        else:
            os.makedirs(f"{self.rawPath}/{subDir}")
            self.metaData = self.metaDataTemplate     
            if self.metaData is not None:   
                with open(self.metaData) as f:
                    self.metaData = yaml.safe_load(f)
        df = helper.lists2DataFrame(source=source,relDir=relDir,fileName=fileName,Interval=Interval)
        for relDir,local in df.groupby('relDir'):
            dest = f"{self.rawPath}/{relDir}"
            if not os.path.isdir(dest):os.makedirs(dest)
            local['Processed'] = False
            local['Template'] = False
            inventory = f"{dest}/fileInventory.csv"
            if os.path.isfile(inventory):
                inventory = pd.read_csv(inventory)
                local = local.loc[~local['source'].isin(inventory['source'])].copy()
                inventory = pd.concat([inventory,local[['Interval','fileName','source']]])
            if not local.empty:
                pb = helper.progressbar(len(local['source']),f'Copying:')
                for (inputFile,fn) in local[['source','fileName']].values:
                    if self.metaData is None:
                        documentFile(inputFile,f"{self.rawPath}/{subDir}",
                                          skiprows=len(fileInfo['pdKwargs']['skiprows']),
                                          headerRows=len(fileInfo['pdKwargs']['header']))
                    if self.mode.lower()=='copy':
                        shutil.copy2(inputFile,f"{dest}/{fn}")
                    elif self.mode.lower()=='move':
                        shutil.move(inputFile,f"{dest}/{fn}")
                    else:
                        print('Invalid mode')
                        sys.exit()
                    pb.step()
                pb.close()
                local.to_csv(inventory,index=False)
                root = local.loc[~local['relDir'].duplicated(),['relDir']].copy()
                root['Processed'] = False
                if self.rawSiteDataInventory.empty:
                    self.rawSiteDataInventory=root
                else:
                    tmp=pd.concat([self.rawSiteDataInventory,root])
                    tmp.loc[tmp['relDir'].duplicated(keep=False),'Processed'] = False
                    self.rawSiteDataInventory = tmp.loc[~tmp.duplicated()].copy()

toDatabaseArgs = {
    'siteID':None,
    'writeCols':None,
    'excludeCols':[],
    'mode':'nafill',
    'stage':None,
    'tag':'',
    'verbose':True
    }

class toDatabase(myProject):
    def __init__(self, projectPath,**kwargs):
        super().__init__(projectPath)
        kwargs = toDatabaseArgs | kwargs
        for k, v in kwargs.items():
            if type(toDatabaseArgs[k])==list and type(v) != list:
                v = [v]
            setattr(self, k, v)
        
        for i,row in self.rawSiteDataInventory.loc[self.rawSiteDataInventory['Processed']==False].iterrows():
            fileType = Path(row['relDir']).parts[1]
            dpath = os.path.abspath(f"{self.rawPath}/{row['relDir']}/")
            progress = pd.read_csv(f"{dpath}/fileInventory.csv")
            self.fileInfo = self.standardInputFileInfo[fileType]
            for ix,file in progress.loc[progress['Processed']==False].iterrows():
                inputFile = os.path.abspath(f"{dpath}/{file['fileName']}")
                try:
                    print(file['fileName'])
                    self.read(inputFile)
                    progress.loc[progress['fileName']==file['fileName'],'Processed']=True
                except Exception as error:
                    print('Could not process: ',file['fileName'])
                    print(error)
            progress.to_csv(f"{dpath}/fileInventory.csv",index=False)
            if progress['Processed'].sum()==progress['Processed'].shape[0]:
                self.rawSiteDataInventory.loc[self.rawSiteDataInventory.index==i,'Processed']=True

        # if inventory.loc[inventory['Processed']==True].shape[0]>0:
        #     self.padFullPeriod()

        
    def read(self,inputFile):
        df = pd.read_csv(inputFile,**self.fileInfo['pdKwargs'])
        # if 'unitParse' in self.fileInfo.keys() and self.fileInfo['unitParse'] and df.columns.nlevels > 1:
        #     print(df.columns.get_level_values(0))
        #     print(df.columns.get_level_values(1))
        # if 'date_format' not in self.fileInfo['pdDateParse'].keys():
        #     self.fileInfo['pdDateParse']['date_format'] = None
        # df.index = pd.to_datetime(
        #     df[df.columns[self.fileInfo['pdDateParse']['cols']]].apply(lambda x: ' '.join(x.dropna().values.tolist()), axis=1),
        #     format = self.fileInfo['pdDateParse']['date_format'])
        # df.index.name = 'TIMESTAMP'
        # if self.writeCols is not None:
        #     df = df[self.writeCols]
        # else:
        #     cols = [c for c in df.columns if c not in [b for a in self.excludeCols for b in fnmatch.filter(df.columns,a)]]
        #     df = df[cols].copy()
        # if not hasattr(self,'colDict'):
        #     self.colDict={}
        # if len(df.columns.levels) > 1:
        #     names = [[c for c in col] for col in df.columns.values]
        #     df.columns = ['_'.join(col).strip() for col in df.columns.values]
        #     for N,c in zip(names,df.columns):
        #         if c not in self.colDict.keys():
        #             self.colDict[c] = {'Name': N[0] } | { 'Unit': n for n in N[1:2] } | { f'Aux{i}': n for i,n in enumerate(N[2:],start=1) }
        # numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        # df_numeric = df.select_dtypes(include=numerics)
        # df_objects = df[df.columns[df.columns.isin(df_numeric.columns)==False]]

        # if hasattr(self,'df_numeric'):
        #     self.df_numeric = pd.concat([self.df_numeric,df_numeric.select_dtypes(include=numerics)])
        # else:
        #     self.df_numeric = df_numeric.copy()
    


    def padFullPeriod(self,period='Y'):
        for p in self.df_numeric.index.to_period(period).unique():
            self.Period = pd.DataFrame(data={
                'TIMESTAMP':pd.date_range(
                start = f'{p}',
                end=f'{p+1}',
                inclusive='right',
                freq=self.df_numeric.index.diff().mean()
                )})
            self.Period = self.Period.set_index('TIMESTAMP')
            self.Period = self.Period.join(self.df_numeric)
            self.Period['POSIX_timestamp'] = self.Period.index.to_series().apply(lambda x: x.timestamp())
            self.write(p)
        
    def write(self,p):
        if self.stage is None:
            db = f"{self.projectPath}/Database/{p}/{self.siteID}/"
        else:
            db = f"{self.projectPath}/Database/{p}/{self.siteID}/{self.stage}/"
        if self.mode.lower() == 'overwrite' and os.path.isdir(db):
            print(f'Overwriting all contents of {db}')
            shutil.rmtree(db)
            os.mkdir(db)
        elif not os.path.isdir(db):
            print(f"{db} does not exist, creating new directory")
            os.makedirs(db)
        for traceName in self.Period.columns:
            if traceName in self.projectConfig["Database"]["Metadata"].keys():
                dt = self.projectConfig["Database"]["Metadata"][traceName]["dtype"]
            else:
                dt = self.projectConfig["Database"]["Metadata"]["Data_traces"]["dtype"]
            fvar = self.Period[traceName].astype(dt).values
            traceName = self.charRep(traceName)
            tracePath = f"{db}{traceName}"
            if os.path.isfile(tracePath):
                trace = np.fromfile(tracePath,dt)
                if self.verbose:
                    print(f'{tracePath} exists, {self.mode} existing file')
            else:
                if self.verbose:
                    print(f'{tracePath} does not exist, writing new file')
                trace = np.empty(self.Period.shape[0],dtype=dt)
                trace[:] = np.nan
            if self.mode.lower() == 'nafill':
                trace[np.isnan(trace)] = fvar[np.isnan(trace)]
            elif self.mode == 'repfill':
                trace[~np.isnan(fvar)] = fvar[~np.isnan(fvar)]
            elif self.mode == 'replace' or self.mode == 'overwrite':
                trace = fvar
            trace.tofile(tracePath)
            
    def charRep(self,traceName):
        # Based on nameParseFields in fr_read_generic_data_file by @znesic, except:
        #   * is replaced with "start" instead of "s"
        #   ( is replaced with ""''"" instead of "_"
        repKey = {'_':[' ','-','.','/'],
                  'star':['*'],
                  '':['(',')'],
                  'p':['%'],
                  }
        if self.tag != '':
            traceName = f"{traceName}_{self.tag}"
        for key,value in repKey.items():
            for val in value:
                traceName = traceName.replace(val,key).split('_Unnamed')[0]
        return(traceName)