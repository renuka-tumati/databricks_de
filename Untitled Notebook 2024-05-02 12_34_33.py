# Databricks notebook source
# Define a decorator function that adds a prefix to the original function name
def add_prefix(prefix):
    def decorator(func):
        # Define a new function that wraps the original function
        def wrapper(*args, **kwargs):
            print(f"Executing function: {prefix}_{func.__name__}")
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Define a function and apply the decorator to it
@add_prefix("LOG")
def greet(name):
    return f"Hello, {name}!"

# Call the decorated function
result = greet("Alice")
print(result)


# COMMAND ----------

def add_prefix(func):
    def wrapper(*args, **kwargs):
        print("Executing function with prefix")
        return func(*args, **kwargs)
    return wrapper

@add_prefix
def greet(name):
    return f"Hello, {name}!"

result = greet("Alice")
print(result)

