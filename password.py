"""
This program asks for a password. if returned correctly, the acces will be granted
"""

def question():
    answer = input("What is the password? " + f"{i} more tries: ")
    return answer


PASSWORD = "test"
i = 3


while i > 0:
    while question() != PASSWORD:
        if i == 1:
            i = i - 1
            break

        print("Wrong! Try Again..")
        i = i - 1

    else:
        print("Success! Access Granted!")
        i = 0
        break
else:
    print("Out of tries")
