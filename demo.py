
import random 

my_list = [True,False]
k = random.choice(my_list)

if k==False:
    raise TypeError("It is intentionally, failed")
else:
    print("Function completed")