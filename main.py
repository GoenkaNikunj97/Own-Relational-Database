import Parse
import constants
from UserLogin import UserLogin
from UserSignUp import UserSignUp
from termcolor import colored



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
    print(colored("     Welcome to Team-8 DataBase Management System      ",'green'))
    print(colored("#####################################################\n",'green'))
    # userLoginSignUp()
    query = ""
    while not query.lower() == "quit":
        print("\n#####################################################")
        database=input(colored("Provide Database Name\n",'blue'))
        if "quit" in database:
            print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Thanks!~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
            exit(0)
        print("\n#####################################################")
        query = input(colored(constants.InputQuery,'blue'))
        query_type = Parse.Parse(database,query)
        val = query_type.check_query()

        if val == -1:
            print(colored("Incorrect Query",'red'))

    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Thanks!~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

if __name__ == "__main__":
    main()
