example_of_int = 10

example_of_str = "10"

example_of_float = 10.01

example_of_dict = {
    "name" : "Juan",
    "age" : 28,
    "scared_or_confident_in_python" : "confident" 
}

example_of_array = [ 1, 2, 3, "Example", "of", "strings", 10.01231, {"name" : "Juan", "age" : 28}] #this is to showcase that you can do store multiple values in an array

# This is known as a comment, programmers use this to document their work in plain english. Comments are completely ignored by the machine when the 
# code is ran. This purely used for documentation purposes. 

# The following lines of code is a progression to showcase the introduction to reading and understanding code. 

# Intro to functions and the 'input' function

value = input("Everything within the quotes is printed to the screen for the user to see and respond. Enter a number: ")
###### This line will be displayed with a cursor at the end, indicating that it is waiting on a response.
###### The reason for this functionality is the function named 'input'. think back to algebra "f(x) = x + 1"
###### Similar to this the function returning a particular OUTPUT (the result after you solved for 'x')
###### there is also an INPUT that needs to be given, this is similar to when you are given x (like 'Solve for x where x = 3')
###### Now you start to see where your algebra classes and above now come in handy. Take a big sigh of relief because you often
###### times will not be asked to do complex math, it is this functionality that you will be using. 

# Now on to reading code. With the brief introduction to values and functions out of the way, you may be tempted to read code
# the same way you read a book. Often times this is fine but you need to be able to recognize how to read code in the order of 
# operation. Similar to how you have the order of operations when solving in math (PEMDAS = Please Excuse My Dear Aunt Sally/ Power Exponent Multiplication Division Add Sub)
# programming has something similar, take this function below for example:

value = int(input("Enter a value: "))
# This is NOT read as follows: "value equals int input and whatever you get back from input.". 
# Instead this read as follows : "the user's input is entered and turned into and int and stored in the value variable".
# There was a progression going from right to left, in the order that the function is required to run. It must started from the right and work its way
# to the right. First, I need the value from the 'input' function in order to pass a value to the 'int' function (think f(x) 
# I cant pass x if i dont have know what it is). Second, now that we received the value from the 'input' function we are not about to pass that value (the 'x' in f(x))
# to the 'int' function. Now int can do its job and return the value as an integer (that is what int does essentially). Finally, we can now assign (this is 
# how you should read a single '=') the value we received from the 'int' function to the 'value' variable.



# Finally, you can see the results of what you just did by using the print function. Again look at it as f(x)
# 'print' is 'f' and 'value' is  'x'. You should the number you entered earlier.
# NOTE: if you did not enter a complete number you will get an error because it is not an integer...
print(value)

#############################################################################################################################################

string_example = "this is just an example of a value"

# What does it mean when we say everything in python is an Object.
# in VS Code and many other text editors, you can press '.' at the end of the 'string_example', it should look like this 
# 'string_example.' there should be a list of possible functions attached to the string that can be used like capitalized. 
# so string_example.capitalize() will make the lowercase 't' into a capital 'T'. 

print(string_example.capitalize())

# This '.' after the variable name is known as dot-notation. It is a way to access the functions within the object. 
# So within 'string_example' there are multiple string functions within it, thats what that list was that you saw after adding the period at the end of 
# the 'string_example' variable. This is what it would actually look like in code:
#
#   string_example = {
#       capitalize() : "The code needed to run this function",
#       lower() : "The code needed to run this function"
#   }
# 
# To see what this looks like under the hood, hold the 'ctrl' key (Windows/Linux) 'command' key (MacOS) and click on capitalize in VS Code. 
# this will take you to the code that, behind the scenes, has been automatically applied to the 'string_example' variable when it was created;
# specifically to the function called 'capitalize()'. The over all structure that your looking at is known as an object. 
# Notice that the capitalize function is under the class 'str'. This is basically saying, the object is of type 'str' or string. 
# The functions that are nested within it are assigned to the class and can be accessed through the object created using '.' dot-notation.
# BUT WAIT, I thought this was an OBJECT not a CLASS. Yes, if this seems confusing, the simple answer that is you need to understand as a beginner is this:
# a CLASS is a function that creates OBJECTS. Ok now how does this look? In other older languages like Java, when you create a string variable, it 
# does not do the same thing. In Java it basically makes a snapshot of the variable in memory(RAM) and it looks somewhat like this:
# 'string_example is of type str', and thats it! There are no functions added under the hood, it is not an object it is just another piece of data
# that the computer is tracking through memory. 

#nested loop = a loop within another loop (outer, inner)


for x in range(1, 10): 
    print(x)

    