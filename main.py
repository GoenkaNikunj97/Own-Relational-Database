import Parse
import constants
from UserLogin import UserLogin
from UserSignUp import UserSignUp
from termcolor import colored
import re
import QueryProcessor as qp

userLogsDir = "UserLogs/UserLogs.json"


def userLoginSignUp():
    existing_member = input("Are you an existing member Y/N: ")
    if existing_member.upper() == "Y" or existing_member.upper() == "N":
        if existing_member.upper() == "Y":
            user_Id = input("Please enter your user ID: ")
            user_Pass = input("Please enter your password: ")
            user_access = UserLogin(user_Id, user_Pass)
            user_access.check()
            if not user_access.login_status:
                userLoginSignUp()
        else:
            user_Id = input("Please enter your user ID: ")
            user_Pass = input("Please enter your password: ")
            user_access = UserSignUp(user_Id, user_Pass)
            user_access.signUp()
            userLoginSignUp()
    else:
        print("\nPlease enter correct options Y/N: \n")
        userLoginSignUp()


def main():

    print(colored("\n#####################################################",'green'))
    print(colored("     Welcome to Team-15 DataBase Management System      ",'green'))
    print(colored("#####################################################\n",'green'))
    userLoginSignUp()
    query = ""
    database=""
    queryProcessor=qp.QueryProcessor()
    while not query.lower() == "quit":
        query=input(constants.InputQuery)
        if "use" in query.lower():
            Parse.Parse.newDB=True
            db_raw=re.compile(r'use\s(.*)\s*',re.IGNORECASE).findall(query)
            database=db_raw[0]
            query=input()
        else:
            database
        query_type = Parse.Parse(database,query,queryProcessor)
        val = query_type.check_query()
        if val == -1:
            print(colored("Incorrect Query", 'red'))
        elif val == 0:
            break

    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Thanks!~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")


if __name__ == "__main__":
    main()
