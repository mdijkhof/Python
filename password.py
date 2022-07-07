def question():
    answer = input("What is the password? "+f"{i} more tries: ")
    return answer


password = "test"
i = 3


while i > 0: 
    while question() != password:
        if i == 1:
            i = i - 1
            break
        else:
            print("Wrong! Try Again..")
            i = i - 1
            break
    else:
        print("Success! Access Granted!")
        i = 0
        break
else:
    print("Out of tries")