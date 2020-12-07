import json
import os


class LockingMechanismChecker:

    def __init__(self):
        self.schemaDirectory = "AllDatabase"
        self.error = "Table is already in locked state"
        self.databaseError = "Database does not exists"
        self.databaseTableError = "Table does not exists for the schema"
        self.noLock = "No lock exists"

    def addLock(self, tableName, database):
        tablePath = self.schemaDirectory + "/" + database + "/" + tableName
        lockFile = self.schemaDirectory + "/" + database + "/" + "lock.json"

        if not os.path.exists(tablePath):
            print(self.databaseTableError)
        else:
            if not os.path.exists(lockFile):
                file = open(lockFile, "w")
                file.write("{\"Lock\":[]}")
                file.close()

            with open(lockFile, "r+") as file:
                lockData = json.load(file)
                temp = lockData['Lock']
                if len(temp) > 0:
                    table_found = False
                    for entry in temp:
                        if tableName == entry['Locked Table']:
                            table_found = True
                            break
                        else:
                            table_found = False
                    if table_found:
                        print(self.error, "-> ", tableName)
                    else:
                        print("\nLock has been added for table: ", tableName)
                        new_lock_entry = {"Locked Table": tableName}
                        temp.append(new_lock_entry)
                else:
                    print("\nLock has been added for table: ", tableName)
                    new_lock_entry = {"Locked Table": tableName}
                    temp.append(new_lock_entry)
            with open(lockFile, 'w') as file:
                json.dump(lockData, file, indent=4)

    def checkLock(self, tableName, database):
        tablePath = self.schemaDirectory + "/" + database + "/" + tableName
        lockFile = self.schemaDirectory + "/" + database + "/" + "lock.json"

        if not os.path.exists(tablePath):
            print(self.databaseTableError)
            return False
        else:
            if not os.path.exists(lockFile):
                file = open(lockFile, "w")
                file.write("{\"Lock\":[]}")
                file.close()
                return False

            with open(lockFile, "r+") as file:
                lockData = json.load(file)
                temp = lockData['Lock']
                if len(temp) > 0:
                    table_found = False
                    for entry in temp:
                        if tableName == entry['Locked Table']:
                            table_found = True
                            break
                        else:
                            table_found = False
                    if table_found:
                        return True
                    else:
                        return False
                else:
                    return False

    def removeLock(self, tableName, database):
        tablePath = self.schemaDirectory + "/" + database + "/" + tableName
        lockFile = self.schemaDirectory + "/" + database + "/" + "lock.json"

        if not os.path.exists(tablePath):
            print(self.databaseTableError)
            return False
        else:
            if not os.path.exists(lockFile):
                file = open(lockFile, "w")
                file.write("{\"Lock\":[]}")
                file.close()
                print(self.noLock)
                return False

            with open(lockFile, "r+") as file:
                lockData = json.load(file)
                temp = lockData['Lock']
                if len(temp) > 0:
                    table_found = False
                    for entry in temp:
                        if tableName == entry['Locked Table']:
                            table_found = True
                            temp.remove(entry)
                            with open(lockFile, 'w') as file:
                                json.dump(lockData, file, indent=4)
                            break
                        else:
                            table_found = False
                    if table_found:
                        return True
                    else:
                        return False
                else:
                    print(self.noLock)
                    return False


if __name__ == "__main__":
    database = "PersonDatabase"
    tableName = "PERSON2"
    obj = LockingMechanismChecker()
    obj.addLock(tableName, database)
    print(obj.checkLock(tableName, database))
    print(obj.removeLock(tableName, database))
