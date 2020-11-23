import constants
import Parse 

def main():
    query=input(constants.InputQuery)

    query_type=Parse.Parse(query)
    val=query_type.check_query()
    
    if(val==-1):
        print ("Incorrect Query")

if __name__ == "__main__":
    main()