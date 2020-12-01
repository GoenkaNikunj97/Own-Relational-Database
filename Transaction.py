import re
from termcolor import colored
import Parse as prs
import QueryProcessor as qp

class Transaction:
    def __init__(self,database,queryProcessor):
        self.database=database
        self.queryProcessor=queryProcessor

    def beginTransaction(self):
        print("\n--------------------------------------------------------")
        print("tranasction started")
        query = ""
        while not query.lower() == "quit":
            query=input()
            query_type = prs.Parse(self.database,query,self.queryProcessor)
            val = query_type.check_query()
            if val == -1:
                print(colored("Incorrect Query",'red'))
            elif val == 0:
                break
        print("transaction ended")
        print("\n--------------------------------------------------------")
        