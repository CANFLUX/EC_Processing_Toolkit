import os
import sys
import json
import psutil
import argparse
import subprocess
import pandas as pd

## Progress bar to update status of a run

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

# def set_high_priority():
#     p = psutil.Process(os.getpid())
#     p.nice(psutil.HIGH_PRIORITY_CLASS)

# def pasteWithSubprocess(source, dest, option = 'copy',Verbose=False,pb=None):
#     set_high_priority()
#     cmd=None
#     source = os.path.abspath(source)
#     dest = os.path.abspath(dest)
#     if sys.platform.startswith("darwin"): 
#         # These need to be tested/flushed out
#         if option == 'copy' or option == 'xcopy':
#             cmd=['cp', source, dest]
#         elif option == 'move':
#             cmd=['mv',source,dest]
#     elif sys.platform.startswith("win"): 
#         cmd=[option, source, dest]
#         if option == 'xcopy':
#             cmd.append('/s')
#     if cmd:
#         proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
#     if pb is not None:
#         pb.step(msg=f"{source}")

#     if Verbose==True:
#         print(proc)

if __name__ == '__main__':
    prefix = 'Test'
    nItems=10
    pb = progressbar(nItems,prefix)
    for i in range(nItems):
        pb.step()
    pb.close()
