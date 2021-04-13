MENU = {
    "espresso": {
        "ingredients": {
            "water": 50,
            "coffee": 18,
        },
        "cost": 1.5,
    },
    "latte": {
        "ingredients": {
            "water": 200,
            "milk": 150,
            "coffee": 24,
        },
        "cost": 2.5,
    },
    "cappuccino": {
        "ingredients": {
            "water": 250,
            "milk": 100,
            "coffee": 24,
        },
        "cost": 3.0,
    }
}
Profit = 0
resources = {
    "water": 300,
    "milk": 200,
    "coffee": 100,
}

def check_resource(ordered_ingredients):
 ingrede_prop = ordered_ingredients["ingredients"]
 for key in ingrede_prop:
   if ingrede_prop[key] >= resources[key]:
     print(f"Sorry we dont have sufficient {key}")
     return False
   else:
     return True

def calculate_collect():
    """Return calculation of coins inserted """
    print("please insert coins")
    quarters = int(input("how many quarters ? :"))
    dimes = int(input("how many dimes ? :"))
    nickles = int(input("how many nickles ? :"))
    pennies = int(input("how many pennies ? :"))
    money_given = float((quarters*0.25) + (dimes*0.10) + (nickles*0.05) + (pennies*0.01))
    return money_given

def balance_calculation(payment, drink_cost):
  """Return true if correct payment made else return sorry message"""
  if payment == drink_cost:
    global Profit
    Profit += drink_cost
    return True
  elif payment > drink_cost:
    change = round(payment - drink_cost, 2)
    Profit += drink_cost
    print(f"Here is your change {change}")
    return True
    # print(Profit)
  else:
    print(f"Sorry insufficient fund {payment}, Money refunded")

def make_coffee(drink, ingredients):
  # print(ingredients)
  ingredients1 = ingredients["ingredients"]
  for key in ingredients1:
    resources[key] -= ingredients1[key]
  print(resources)

def report():
  for key in resources:
    print(f"{key.title()} : {resources[key]} ")
  print(f"Money : $ {Profit}")

is_on = True
while is_on == True:
  customer_choice = (input("What you would like to have ? (Expresso/capuccino/Latte): ")).lower()
  if customer_choice == "off":
    is_on = False
  elif customer_choice == "report":
    report()
  else:
    if(check_resource(MENU[customer_choice])):
      payment = calculate_collect()
      drink_cost = MENU[customer_choice]["cost"]
      if (balance_calculation(payment, drink_cost)):
        make_coffee(customer_choice, MENU[customer_choice])

      
