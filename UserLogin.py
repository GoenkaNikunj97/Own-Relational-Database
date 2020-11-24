import json
import os


class UserLogin:
    def __init__(self, userId, password):
        self.userId = userId
        self.password = password
        self.credentialsDir = "Credentials/UserCredentials.json"
        self.error = "Please enter valid credentials"
        self.missingDirectory = "Sorry you are not registered. Kindly register"
        self.login_status = False

    def check(self):

        if os.path.exists(self.credentialsDir):
            with open(self.credentialsDir) as file:
                credentialsData = json.load(file)
                if len(credentialsData) > 0:
                    user_found = False
                    pass_matched = False
                    for entry in credentialsData:
                        if entry["userID"] == self.userId:
                            user_found = True
                            if entry["password"] == self.password:
                                pass_matched = True
                            else:
                                pass_matched = False
                            if user_found and pass_matched:
                                print("\n#####################################################")
                                print("             Welcome to the DBMS: " + self.userId + "                ")
                                print("#####################################################\n")
                                self.login_status = True
                                break
                            elif user_found:
                                print("\nSorry password is incorrect!!! Re-enter credentials\n")
                                break
                        else:
                            user_found = False
                    if not user_found:
                        print("\nSorry no information found!!! Kindly register\n")
                        self.login_status = False
                else:
                    print("\nSorry no information found\n")
                    self.login_status = False
        else:
            print(self.missingDirectory)