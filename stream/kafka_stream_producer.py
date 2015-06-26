##############################################################################################
#
# Script to generate user activity stream
# Parameters:
#   -sid <starting userid number>
#   -eid <ending userid number>
#   -s <message interval in seconds>
#
##############################################################################################

__author__ = 'K'

from faker import Factory
from datetime import datetime, timedelta
import argparse
from random import randint
import random
import math
import time
import glob, os
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

# Read location information of all cities in california
def read_locations():
    locations = []
    # Open locations.csv file
    # Header: ZipCode,Latitude,Longitude,City,State,County
    with open('Locations.csv', 'rU') as f:
        content = f.readlines()
    header = None
    rownum = 0
    # Loop through all the lines in file
    for row in content:
        # Save header line
        if rownum == 0:
            header = row.strip().split(',')
        # For non header line, parse out fields
        else:
            colnum = 0
            location_data = {}
            # Loop through fields and create location_data structure
            for col in row.strip().split(','):
                location_data[header[colnum]] = col
                colnum += 1
            locations.append(location_data)
        rownum += 1
    # Return all the locations
    return locations

# Giver current time return what even user is doing based on schedule created
def create_event(current_time, schedule, user_id, name, user_location):
    # If no activity then set to IDLE
    activityType = 'IDLE'
    activityStartTime = None

    # If user has running activity that day
    if schedule['hasRunningActivity']:
        # If current time is within running activity, set activity type to RUNNING
        if schedule['runningStart'] <= current_time <= schedule['runningEnd']:
            activityType = 'RUNNING'
            activityStartTime = schedule['runningStart']
    # If user has cycling activity that day
    if schedule['hasCyclingActivity']:
        # If current time is within cycling activity, set activity type to CYCLING
        if schedule['cyclingStart'] <= current_time <= schedule['cyclingEnd']:
            activityType = 'CYCLING'
            activityStartTime = schedule['cyclingStart']
    # If user has walkning activity that day
    if schedule['hasWalkingActivity']:
        # If current time is within first walking activity start and last activity end
        if schedule['walkingStart'] <= current_time <= schedule['walkingEnd']:
            # Go through all walking activities and set activity type to WALKING
            # if current time falls under the range
            for i in range(0, schedule['totalTimesWalked']):
                if schedule['walkingStart' + `i`] <= current_time <= schedule['walkingEnd' + `i`]:
                    activityType = 'WALKING'
                    activityStartTime = schedule['walkingStart' + `i`]
    # If current time is before wakeup or after sleep set activity type to SLEEPING
    if current_time > schedule['sleep'] or current_time < schedule['wakeup']:
        activityType = 'SLEEPING'

    # Construct json for a given activity details
    return {
        "user_id": user_id,
        "name": name,
        "time": timestamp_serializable(current_time),
        "activity_group_id": 0 if activityStartTime is None else timestamp_serializable(activityStartTime),
        "activity_type": activityType,
        "lat": user_location['Latitude'],
        "lon": user_location['Longitude'],
        "zip": user_location['ZipCode'],
        "city": user_location['City']
    }

# Create random schedule for a given date
def create_schedule(fake, dt):
    # Get random wakeup time within morning time range
    wakeup_time = get_wakeup_time(fake, dt)
    # Get random sleep time within night time range
    sleep_time = get_sleep_time(fake, dt)
    distribution = randint(0, 9)
    schedule = {}

    # Start with no activity on the day
    schedule['hasRunningActivity'] = False
    schedule['hasCyclingActivity'] = False
    schedule['hasWalkingActivity'] = False

    # From distribution set certain activities based on probability
    # Max 10% people cycle on a given day
    if distribution == 0:
        add_cycling_activity(fake, schedule, dt, wakeup_time, sleep_time)
    # Max 20% people run on a given day
    elif distribution >= 1 and distribution <= 2:
        add_running_activity(fake, schedule, dt, wakeup_time, sleep_time)
    # Max 1% people run and cycle on a given day (10% of a 10%)
    elif distribution == 3:
        distribution = randint(0, 9)
        if distribution == 0:
            add_cycling_activity(fake, schedule, dt, wakeup_time, sleep_time)
            add_running_activity(fake, schedule, dt, wakeup_time, sleep_time)

    # Max 90% people walk on a given day
    distribution = randint(0, 9)
    if distribution < 9:
        add_walking_activity(schedule, wakeup_time, sleep_time)

    # Set wakeup and sleep time in schedule
    schedule['wakeup'] = wakeup_time
    schedule['sleep'] = sleep_time
    return schedule

# Create walking activities throughout the day
def add_walking_activity(schedule, wakeup_time, sleep_time):
    # Only add walking activity after few hours from waking up
    current_time = wakeup_time+timedelta(hours=randint(1, 3))
    # Start with no activity on that day
    schedule['hasWalkingActivity'] = False
    # Expected number of walking activities are 0-5
    expectedNumberOfTimesWalked = randint(0, 5)
    # Start with 0 number of walking activities on that day
    totalNumberOfTimesWalked = 0

    # Create walking activites based on expected random number of activities
    for i in range(0, expectedNumberOfTimesWalked):
        # Start walking activity somewhere within 2 hours of given current time
        walking_start = current_time + timedelta(hours=randint(0,1), minutes=randint(0,59))

        # If this is first walking activity in the day, note that
        if i == 0:
            schedule['hasWalkingActivity'] = True
            schedule['walkingStart'] = walking_start

        distribution = randint(0, 9)
        # 70% of walks will be shorter than 30 mins
        if distribution < 7:
            walking_end = walking_start + timedelta(minutes=30)
        # 30% of walks will be upto 90 mins
        else:
            walking_end = walking_start + timedelta(minutes=90)

        # If walking ends before sleep time then only consider it, and add to schedule
        if walking_end < (sleep_time + timedelta(hours=-1)):
            schedule['walkingStart' + `i`] = walking_start
            schedule['walkingEnd' + `i`] = walking_end
            totalNumberOfTimesWalked += 1
            # Set the walking activity end of whole day
            schedule['walkingEnd'] = walking_end
        else:
            break

        # Next walking activity should be within next 4 hours of current walking activity
        current_time = walking_end + timedelta(hours=randint(0,3), minutes=randint(0,59))

    # Set how many times walking on the day
    schedule['totalTimesWalked'] = totalNumberOfTimesWalked

# Create running activity for the day, only one activity per day
def add_running_activity(fake, schedule, dt, wakeup_time, sleep_time):
    # Start with no running activity for the day
    hasRunningActivity = False
    running_start_time = None
    running_end_time = None

    # If this is a weekday, probability of running is 70%
    if dt.weekday() < 5:
        distribution = randint(0, 9)
        if distribution < 7:
            # Duration of running activity should be upto 2 hours
            running_start_time = get_running_start_time(fake, wakeup_time, sleep_time)
            running_end_time = running_start_time + timedelta(hours=randint(0,1), minutes=randint(0,59))
            hasRunningActivity = True
    # If this is weekend, probability of running is 90%
    else:
        distribution = randint(0, 9)
        if distribution < 9:
            # Duration of running activity should be upto 3 hours
            running_start_time = get_running_start_time(fake, wakeup_time, sleep_time)
            running_end_time = running_start_time + timedelta(hours=randint(0,2), minutes=randint(0,59))
            hasRunningActivity = True

    # Set activity in schedule
    schedule['hasRunningActivity'] = hasRunningActivity
    schedule['runningStart'] = running_start_time
    schedule['runningEnd'] = running_end_time

# Create cycling activity for the day, only one activity per day
def add_cycling_activity(fake, schedule, dt, wakeup_time, sleep_time):
    # Start with no cycling activity for the day
    hasCyclingActivity = False
    cycling_start_time = None
    cycling_end_time = None

    # If this is a weekday, probability of cycling is 70%
    if dt.weekday() < 5: # week day
        distribution = randint(0, 9)
        if distribution < 7:
            # Duration of cycling activity should be upto 2 hours
            cycling_start_time = get_cycling_start_time(fake, wakeup_time, sleep_time)
            cycling_end_time = cycling_start_time + timedelta(hours=randint(0,1), minutes=randint(0,59))
            hasCyclingActivity = True
    # If this is weekend, probability of running is 90%
    else:
        distribution = randint(0, 9)
        if distribution < 9:
            # Duration of cycling activity should be upto 3 hours
            cycling_start_time = get_cycling_start_time(fake, wakeup_time, sleep_time)
            cycling_end_time = cycling_start_time + timedelta(hours=randint(0,2), minutes=randint(0,59))
            hasCyclingActivity = True
    schedule['hasCyclingActivity'] = hasCyclingActivity
    schedule['cyclingStart'] = cycling_start_time
    schedule['cyclingEnd'] = cycling_end_time

# Get running start time based on probability morning vs. evening
def get_running_start_time(fake, wakeup_time, sleep_time):
    distribution = randint(0, 9)
    # 30% people run in morning
    if distribution < 3:
        return fake.date_time_between(start_date=wakeup_time+timedelta(hours=1), end_date=wakeup_time+timedelta(hours=4))
    # 70% people run in evening
    else:
        return fake.date_time_between(start_date=sleep_time+timedelta(hours=-5), end_date=sleep_time+timedelta(hours=-1))

# Get cycling start time based on probability morning vs. evening
def get_cycling_start_time(fake, wakeup_time, sleep_time):
    distribution = randint(0, 9)
    # 30% people cycle in morning
    if distribution < 3:
        return fake.date_time_between(start_date=wakeup_time+timedelta(hours=1), end_date=wakeup_time+timedelta(hours=4))
    # 70% people cycle in evening
    else:
        return fake.date_time_between(start_date=sleep_time+timedelta(hours=-5), end_date=sleep_time+timedelta(hours=-1))

# Get start of a day, 0 AM
def get_start_of_day(dt):
    return datetime(dt.year, dt.month, dt.day)

# Get end of a day, 11:59:59PM
def get_end_of_day(dt):
    return get_start_of_day(dt) + timedelta(hours=23, minutes=59, seconds=59)

# Get random wakeup time between 5-10 AM
def get_wakeup_time(fake, dt):
    start_of_day = get_start_of_day(dt)
    return fake.date_time_between(start_date=start_of_day+timedelta(hours=5), end_date=start_of_day+timedelta(hours=10))

# Get random sleep time between 7PM-midnight
def get_sleep_time(fake, dt):
    return fake.date_time_between(start_date=get_start_of_day(dt)+timedelta(hours=19), end_date=get_end_of_day(dt))

# Get formated string for date
def datetime_serializable(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# Get timestamp of the date
def timestamp_serializable(dt):
    return dt.strftime("%s")

# Given a location in lat,long produce near by location in given radius
def get_random_location_around(location, radius):
    radius_in_degree = radius/111300
    u = float(random.uniform(0.0, 0.1))
    v = float(random.uniform(0.0, 0.1))
    w = radius_in_degree * math.sqrt(u)
    t = 2 * math.pi * v
    x = w * math.cos(t)
    y = w * math.sin(t)
    return {
        'Latitude': location['Latitude'] + x,
        'Longitude': location['Longitude'] + y
    }

# Serialize event to json
def get_event_json(event):
    return '{{"user_id": "{0:08d}", "name": "{1}", "time": "{2}", "activity_group_id": "{3}", "activity_type": "{4}", "lat": "{5}", "lon": "{6}", "zip": "{7}", "city": "{8}"}}'.format(
        event['user_id'],
        event['name'],
        event['time'],
        event['activity_group_id'],
        event['activity_type'],
        event['lat'],
        event['lon'],
        event['zip'],
        event['city']
    )

# Main function
if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-sid", help="start user id")
    parser.add_argument("-eid", help="end user id")
    parser.add_argument("-s", help="wait time in seconds before generating next event")
    args = parser.parse_args()

    # Kafka
    client = KafkaClient("ec2-52-26-58-1.us-west-2.compute.amazonaws.com:9092")
    producer = SimpleProducer(client)

    fake = Factory.create()
    start_user_id = int(args.sid)
    end_user_id = int(args.eid)
    wait_time = int(args.s)

    # User infos
    user_schedules = {}
    user_names = {}
    user_locations = {}
    user_prev_events = {}

    # Read city locations
    locations = read_locations()
    locations_count = len(locations)-1

    # Selected user names
    names = {}

    # Produce activity stream for each user between start and end user id
    # supplied in command line
    for user_id in range(start_user_id, end_user_id):
        # Create random name for user
        name = fake.name()

        # Keep looking for unique name
        while name in names:
            name = fake.name()

        # Add selected name in the dictionary
        names[name] = 1
        user_names[user_id] = name

        # Randomly assign location to user
        loc = locations[randint(0, locations_count)]
        user_location = get_random_location_around({
            'Latitude': float(loc['Latitude']),
            'Longitude': float(loc['Longitude'])
        }, 3000)
        user_location['ZipCode'] = loc['ZipCode']
        user_location['City'] = loc['City']
        user_locations[user_id] = user_location

        # Create schedule for today
        schedule = create_schedule(fake, datetime.now())

        # Add to user schedules
        user_schedules[user_id] = schedule

    while 1:
        # Every second generate events for each user
        for user_id in range(start_user_id, end_user_id):
            # Get current time
            current_time = datetime.now()
            # Create event for current time
            event = create_event(current_time, user_schedules[user_id], user_id, user_names[user_id], user_locations[user_id])
            # Set previous event for the user
            if user_id in user_prev_events:
                if user_prev_events[user_id]['activity_type'] is not user_prev_events[user_id]['activity_type']:
                    current_activity_start_time = user_prev_events[user_id]['time']
            else:
                current_activity_start_time = timestamp_serializable(current_time)
            if event['activity_group_id'] is 0:
                event['activity_group_id'] = current_activity_start_time
            # Event json
            event_json = get_event_json(event)
            print event_json
            producer.send_messages('activity_stream', event_json)
            user_prev_events[user_id] = event
        # Wait a bit
        time.sleep(wait_time)




