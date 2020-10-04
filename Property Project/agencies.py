import psycopg2
import json
from user_functions import VarIf, convertJSONDate, getKeyValue, QueryWithSingleValue, returnNextSerialID, cleanStringForInsert, checkRowExists
from database import cur, conn
from user_constants import *


### Agency Storage Functions

### End Agency Storage Functions

def storeAgencies( agencyObject ):
    