import re
from termcolor import colored
import Parse as prs
class Transaction:
    def __init__(self):
        return
    def beginTransaction(self):
        print("\n--------------------------------------------------------")
        print("tranasction started")
        query = ""
        database=""

        while not query.lower() == "quit":
            query=input()
            if "use" in query.lower():
                db_raw=re.compile(r'use\s(.*)\s*',re.IGNORECASE).findall(query)
                database=db_raw[0]
                query=input()
            else:
                database
            query_type = prs.Parse(database,query)
            val = query_type.check_query()
            if val == -1:
                print(colored("Incorrect Query",'red'))
            elif val == 0:
                break
        print("transaction ended")
        print("\n--------------------------------------------------------")
        