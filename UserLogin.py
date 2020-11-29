import base64
import json
import os
from datetime import datetime


def decodingPassword(password):
    decrpyt_password = base64.b64encode(bytes(password, 'utf-8'))
    return decrpyt_password


class UserLogin:
    def __init__(self, userId, password):
        self.userId = userId
        self.password = password
        self.credentialsDir = "Credentials/UserCredentials.json"
        self.error = "Please enter valid credentials"
        self.missingDirectory = "Sorry you are not registered. Kindly register"
        self.login_status = False
        self.timeStamp = None

    def check(self):

        if os.path.exists(self.credentialsDir):
            with open(self.credentialsDir) as file:
                userLogsData = json.load(file)
                if len(userLogsData) > 0:
                    user_found = False
                    pass_matched = False
                    for entry in userLogsData:
                        if entry["userID"] == self.userId:
                            user_found = True
                            if entry["password"] == str(decodingPassword(self.password)):
                                pass_matched = True
                            else:
                                pass_matched = False
                            if user_found and pass_matched:
                                print("\n#####################################################")
                                print("             Welcome to the DBMS: " + self.userId + "                ")
                                print("#####################################################\n")
                                self.timeStamp = datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")
                                self.login_status = True
                                break
                            elif user_found:
                                print("\nSorry password is incorrect!!! Re-enter credentials\n")
                                exit(0)
                                break
                        else:
                            user_found = False
                    if not user_found:
                        print("\nSorry no information found!!! Kindly register\n")
                        exit(0)
                        self.login_status = False
                else:
                    print("\nSorry no information found\n")
                    self.login_status = False
        else:
            print(self.missingDirectory)

    def logs(self):
        return self.userId + "," + self.timeStamp
