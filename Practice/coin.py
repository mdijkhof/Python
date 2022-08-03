import random
#from random import choice

# coin = choice(["Heads","Tails"])
# print(coin)

# x = random.choice(range(4))
# print(x)

# i = 0
# for i in range(10):
#     if i < 10:
#         number = random.randint(1,10)
#         print(number)
#         i = i + 1
        
        
cards = ["jack","queen","king"]
random.shuffle(cards)
for card in cards:
    print(card)