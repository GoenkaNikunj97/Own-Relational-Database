import json
from datetime import datetime
import os
class Logging:
    def __init__(self):
        self.logsFileFullPath = "Logs/eventLogs.json"
    def pushLog(self,query,message):
        try:
            if message=="":
                message="Error in Query Execution"
            now = datetime.now()
            if  not os.path.exists(self.logsFileFullPath):
                f = open(self.logsFileFullPath, "w")
                writeStr="{\"logs\":[]}"
                f.write(writeStr)
                f.close()
            new_entry = {"Execution Time":now.strftime("%Y-%m-%d %H:%M:%S"),"query":query,"message":message}
            with open(self.logsFileFullPath, "r+") as file:
                Data = json.load(file)  
                temp=Data['logs']
                temp.append(new_entry)
            with open(self.logsFileFullPath,'w') as f:  
                json.dump(Data, f,indent=4)
        except Exception as e:
            print(e)

