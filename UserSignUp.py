import json
import os
import base64


def encodingPassword(password):
    encrpyt_password = base64.b64encode(bytes(password, 'utf-8'))
    return encrpyt_password

class UserSignUp:
    def __init__(self, userId, password):
        self.userId = userId
        self.password = password
        self.credentialsDir = "Credentials/"
        self.credentialsFileFullPath = "Credentials/UserCredentials.json"
        self.error = "Please enter valid credentials"
        self.missingDirectory = "Credentials directory does not exists"
        self.missingFile = "UserCredential file does not exists"

    def signUp(self):

        if not os.path.exists(self.credentialsFileFullPath):
            f = open(self.credentialsFileFullPath, "w")
            f.write("[]")
            f.close()

        encrypted_password = encodingPassword(self.password)
        new_entry = {'userID': self.userId, 'password': str(encrypted_password)}
        with open(self.credentialsFileFullPath, "r+") as file:
            credentialsData = json.load(file)
            if len(credentialsData) > 0:
                user_found = False
                for entry in credentialsData:
                    if entry["userID"] == self.userId:
                        user_found = True
                        break
                    else:
                        user_found = False
                if user_found:
                    print("\nusername already exists\n")
                else:
                    print("\nYour Credentials has been added\n")
                    credentialsData.append(new_entry)
                    file.seek(0)
                    json.dump(credentialsData, file)
            else:
                print("\nYour Credentials has been added\n")
                credentialsData.append(new_entry)
                file.seek(0)
                json.dump(credentialsData, file)
