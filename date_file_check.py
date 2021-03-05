import subprocess
from datetime import date
today = date.today()
# dd/mm/YY
today_date = today.strftime("%d/%m/%Y")
record_date = subprocess.check_output(['head', '-1', 'test.txt'])
record_size_line = subprocess.check_output(['tail', '-1', 'test.txt'])
record_size = record_size_line.split(':')[1]
print(record_size)
file = open("test.txt","r") 
Counter = 0
# Reading from file 
Content = file.read() 
CoList = Content.split("\n") 
for i in CoList: 
    if i: 
        Counter += 1
Required_lines = Counter -2
print(Required_lines)
if [(record_date == today_date) and (record_size == Required_lines)]:
    print("all good")
else:
    print("something fishy")
