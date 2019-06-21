import actions
from datetime import datetime, timedelta
import shutil
from os import mkdir, chdir, getcwd, remove

database_name = 'KM_new_prop.db'

dir_name = database_name.split('.')[0]
dir = '../'+dir_name
try:
    shutil.rmtree(dir, ignore_errors=True)
    mkdir(dir)
    shutil.copyfile('../clean_database/clean_KM_newprop.db', dir+'/'+database_name)
except FileExistsError:
    pass

chdir(dir)
print(database_name, getcwd())
database = actions.Database(database_name)

start_date = database.find_earliest_date()
#print(start_date)
current_date = start_date
end_date = datetime(year=2029, month=6, day=12, hour=0, minute=0, second=0)

last_forecast = start_date
time_since_forecast = timedelta(days=0)
limit = timedelta(days=28)
#database.transit_forecast(start_date, start_date+timedelta(days=28))
while current_date < end_date:
    current_date += timedelta(days=7)
    database.make_schedules(current_date)
    database.simulate_observations(current_date, current_date+timedelta(days=7))
    time_since_forecast += timedelta(days=7)
    if time_since_forecast >= limit:
        print('Forecasting on:', current_date)
        database.transit_forecast(current_date, current_date + timedelta(days=28))
        time_since_forecast = timedelta(days=0)

