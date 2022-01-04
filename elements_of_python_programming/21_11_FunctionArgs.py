def foo(x=[]):
    x.append(1)
    return x


if __name__ == '__main__':
   result = foo()
   print(result)
   result = foo()
   print(result)
