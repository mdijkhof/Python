from sayings import hello

def test_hello():
    assert hello("David") == "hello, David"
    assert hello() == "hello, "
    
    # run it by running pytest test_hello.py instead of py test_hello.py