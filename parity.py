def main():
    x = int(input("What's x? "))
    if parity_v3(x):
        print("Even")
    else:
        print("Odd")

""""
def parity(n):
    if n % 2 == 0:
        return True
    else:
        return False
"""


def parity_v2(n):
    return True if n % 2 == 0 else False


def parity_v3(n):
    return n % 2 == 0


main()
