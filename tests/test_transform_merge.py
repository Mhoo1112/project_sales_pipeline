import re

string = "Transaction ID: TX-123456 | Status: SUCCESS | User: a; Transaction ID: TX-987654 | Status: FAILURE | User: b; ID: 112233"
# pattern = r'(\d{4}|\d{2})[-\/]+(\d{2})[-\/]+(\d{4}|\d{2})'
pattern = r'(TX-[0-9]{6}).*?(SUCCESS|FAILURE)'

match = re.findall(pattern, string)

if match:
    print(f"{match}")