# constant variable is capitalized
MEOWS = 3

for _ in range(MEOWS):
    print("meow")

class Cat:
    MEOWS = 3
    
    def meow(self):
        for _ in range(Cat.MEOWS):
            print("meow")
            
cat = Cat()
cat.meow()
