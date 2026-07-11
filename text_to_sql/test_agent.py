import os
import sys
sys.path.insert(0, '.')
from dotenv import load_dotenv
load_dotenv('../.env')

from app.agent import generate_sql

result = generate_sql('How many yellow taxi trips were there in April 2020?')
print('Valid:', result['valid'])
print('Error:', result['error'])
print('SQL:', result['sql'])
print('Explanation:', result['explanation'])