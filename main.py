import json
import os
from datetime import datetime

import Parse
import constants
from UserLogin import UserLogin
from UserSignUp import UserSignUp

userLogsDir = "UserLogs/UserLogs.json"


def userLoginSignUp():
    existing_member = input("Are you an existing member Y/N: ")
    if existing_member.upper() == "Y" or existing_member.upper() == "N":
        if existing_member.upper() == "Y":
            user_Id = input("Please enter your user ID: ")
            user_Pass = input("Please enter your password: ")
            user_access = UserLogin(user_Id, user_Pass)
            user_access.check()
            userLogs = user_access.logs()
            return userLogs
        else:
            user_Id = input("Please enter your user ID: ")
            user_Pass = input("Please enter your password: ")
            user_access = UserSignUp(user_Id, user_Pass)
            user_access.signUp()
            exit(0)
            # userLoginSignUp()
    else:
        print("\nPlease enter correct options Y/N: \n")
        userLoginSignUp()


def main():
    print("\n#####################################################")
    print("     Welcome to Team-8 DataBase Management System      ")
    print("#####################################################\n")
    userLogs = userLoginSignUp()
    queryList = []
    queryTimeStamp = []
    query = ""

    while not query.lower() == "quit":
        query = input(constants.InputQuery)
        query_type = Parse.Parse(query)
        val = query_type.check_query()
        queryList.append(query)
        queryTimeStamp.append(datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"))

        if val == -1:
            print("Incorrect Query")

    user_loginInfo = userLogs.split(',')
    userLog_Entry = []
    for query, timestamp in zip(queryList, queryTimeStamp):
        query_entry = {'Query': query, 'TimeStamp': timestamp}
        userLog_Entry.append(query_entry)

    user_logs = {'userId': user_loginInfo[0], 'loginTimestamp': user_loginInfo[1], 'queries': userLog_Entry}

    if not os.path.exists(userLogsDir):
        f = open(userLogsDir, "w")
        f.write("[]")
        f.close()

    with open(userLogsDir, "r+") as file:
        userLogsData = json.load(file)
        if len(userLogsData) > 0:
            print("\nUser Log registered\n")
            userLogsData.append(user_logs)
            file.seek(0)
            json.dump(userLogsData, file)
        else:
            print("\nUser Log registered successfully\n")
            userLogsData.append(user_logs)
            file.seek(0)
            json.dump(userLogsData, file)


if __name__ == "__main__":
    main()
