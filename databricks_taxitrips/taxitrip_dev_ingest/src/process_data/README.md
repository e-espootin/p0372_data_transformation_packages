	1.  Extract Time-based Features:
	[x]	    Pickup Hour: Derive an hour-of-day column from lpep_pickup_datetime.
	[x]	    Pidckup Day of Week: Derive a weekday name/number (Mon, Tue, etc.) from lpep_pickup_datetime.
	[x]	    Trip Duration (Minutes): Calculate the difference between lpep_dropoff_datetime and lpep_pickup_datetime.      
    [x]       Request-to-Pickup Delay: Calculate time difference between request_datetime and lpep_pickup_datetime to understand wait times.
    . Pickup Month / Season: Extract month or season (Winter, Spring, Summer, Autumn) for seasonal trend analysis.
	2.	Geospatial Features:
	[x]	    Trip Distance (Existing): Already given, but you could create a “Trip Distance (Miles/Km)” if needed.
	•	    Pickup/Drop-off Borough or Zone: Map PULocationID and DOLocationID to borough or zone names via a lookup.
    .       Pickup/Drop-off Neighborhood: Map coordinates or IDs to neighborhoods, creating a “pickup_neighborhood” or “dropoff_neighborhood” column
    .       Airport Trips: Flag trips starting or ending at airports (e.g., is_airport_trip = True) by checking if location IDs or lat/long match airport areas.
	3.	Fare and Cost Analysis:
	[x]	    Total Tip Percentage: (tip_amount / fare_amount) * 100 to understand tipping behavior.
	•	    Net Fare (Fare - Tolls - Surcharges): Useful to see the base driver earning potential.
    [x]    Fare per Distance: (fare_amount / trip_distance) to understand pricing strategy.
    [x]    Revenue per Minute: (fare_amount / trip_duration_minutes) to see earnings efficiency.
	4.	Weather & Environmental Features:
	•	    Is High Humidity (Yes/No): Based on humidity > certain threshold.
	•	    Is Extreme Weather (Yes/No): Based on weather_description (e.g. “Heavy Rain”).
	[x]	    Day/Night Indicator: Compare pickup_hour to a set threshold (e.g. <6 or >20 = Night).
    [x]       Is Rush Hour (Yes/No): Based on pickup_hour (e.g. 7-9 AM, 5-7 PM).
    .       Feels-Like Difference: feels_like - temperature to understand how much weather conditions differ from the actual temperature.
    .       
	5.	Passenger/Driver Features:
	•	    Passenger Demographics: Create categories based on passenger_age, passenger_sex, or passenger_Job. For instance, “Young 
    Professional” if age <30 and has a certain job title.
    [x]       Passenger Loyalty Indicator: Check if the same passenger_email appears multiple times. If so, you can count total trips per passenger and create a “trip_count_for_passenger”.

    [x]       Driver Experience Level: Count trips per driver_email over the dataset to indicate “experienced_driver” if over a certain threshold.
    6. Distance & Speed Metrics:
	[x]	Average Speed: (trip_distance / (trip_duration_minutes / 60)) for km/h or mph.
    .   Traffic Congestion Level: If external traffic data is available, create a congestion score for pickup time/location.