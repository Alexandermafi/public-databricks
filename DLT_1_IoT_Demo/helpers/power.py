def n_to_mth(n: float, m:int) -> None:
  """
  Prints the result of raising a number 'n' to the power 'm'.

  This function takes two numbers as input: 'n', the base number, and 'm', the exponent. 
  It calculates the power of 'n' raised to 'm' and prints the result in a formatted string.

  Parameters:
  - n (float): The base number. Although integers are also acceptable, float is used to accommodate all numeric types.
  - m (int): The exponent to which the base number is raised.

  Returns:
  - None: This function does not return a value. It prints the result to the console.

  Example:
  >>> n_to_mth(3, 2)
  3 to the 2th power is 9
  """
  print(n, "to the", m,"th power is", n**m)