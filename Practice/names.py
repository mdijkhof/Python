name = input("What's your name? ")

# # basic version where file needs to be closed
# file = open("names.txt","a")
# file.write(f"{name}\n")
# file.close()


# # improved option and don't need to be closed
# with open("names.txt","a") as file:
#     file.write(f"{name}\n") 
#     file.close()
    

# read file and read lines in file    
with open("names.txt","r") as file:
    lines = file.readlines()
    
for line in lines:
    # print("Hello,",line,end="")
    print("Hello,",line.rstrip())
    
    
# read file and read lines in file but shorter, this can't be sorted though
with open("names.txt", "r") as file:
    for line in file:
        print("hello,",line.rstrip())
        
        
# now sort it. no "r" as read is the default open, also, appending names from the file to the list
names = []

with open("names.txt") as file:
    for line in file:
        names.append(line.rstrip())
        
for name in names:
    print(f"hello, {name}")
    