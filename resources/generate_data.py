import pandas as pd
import random
from datetime import date, timedelta

# Generate flightData.csv
flight_data = {
    "passengerId": [random.randint(1, 100) for _ in range(1000)],
    "flightId": [i for i in range(1000)],
    "from": [random.choice(['US', 'UK', 'CA', 'DE', 'FR', 'CN']) for _ in range(1000)],
    "to": [random.choice(['US', 'UK', 'CA', 'DE', 'FR', 'CN']) for _ in range(1000)],
    "date": [(date(2023, 1, 1) + timedelta(days=random.randint(0, 364))).strftime('%Y-%m-%d') for _ in range(1000)]
}
df_flight = pd.DataFrame(flight_data)
df_flight.to_csv('flightData.csv', index=False)

# Generate passengers.csv
passenger_data = {
    "passengerId": [i for i in range(1, 101)],
    "firstName": ["FirstName" + str(i) for i in range(1, 101)],
    "lastName": ["LastName" + str(i) for i in range(1, 101)]
}
df_passenger = pd.DataFrame(passenger_data)
df_passenger.to_csv('passengers.csv', index=False)
