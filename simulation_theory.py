import actions
from datetime import datetime, timedelta
import shutil
from os import mkdir, chdir, getcwd
import database_generator
import argparse

parser = argparse.ArgumentParser(description='Run simulation')
parser.add_argument('telescopes', type=str, help='file containing telescope data for simulation')
parser.add_argument('mode', type=str, help='Operating mode')
parser.add_argument('threshold', type=int, help='Accuracy threshold')
args = parser.parse_args()
telescope_file = '../telescopes/'+args.telescopes
mode = args.mode
threshold = args.threshold

sim_name = telescope_file.split('/')[-1].split('.')[0] + '_' + mode + '_' + str(threshold)
#telescope_file = '../telescopes/telescopes.csv'
#mode = 'unlimited'
database_name = sim_name+'.db'
result_file = sim_name+'.csv'
print(sim_name, mode, threshold)

dir_name = database_name.split('.')[0]
try:
    shutil.rmtree(dir_name, ignore_errors=True)
    mkdir(dir_name)
    shutil.copyfile('clean/clean3.db', dir_name+'/'+database_name)
except FileExistsError:
    pass

chdir(dir_name)
database = actions.Database(database_name, telescope_file, mode, threshold)
telescopes = database_generator.generate_sql_table_from_csv(telescope_file, 'TELESCOPES', database.cursor)
for telescope in telescopes:
    database.cursor.execute('DROP TABLE IF EXISTS ' + telescope[0])
    database.cursor.execute('CREATE TABLE IF NOT EXISTS ' + telescope[
        0] + '(Target VARCHAR(25), RA DECIMAL(16,8), Dec DECIMAL(16,8), ObsCenter DATETIME, RunStart DATETIME, RunEnd DATETIME, RunDuration TIME, Epoch REAL, UNIQUE(RunStart))')
    print(telescope[0])
database.db.commit()


start_date = database.find_earliest_date()
#print(start_date)
current_date = start_date
end_date = datetime(year=2030, month=6, day=12, hour=0, minute=0, second=0)

last_forecast = start_date
time_since_forecast = timedelta(days=0)
limit = timedelta(days=28)

totals = []
counts = []
dates = []
#database.transit_forecast(start_date, start_date+timedelta(days=28))
while current_date < end_date:
    current_date += timedelta(days=7)
    database.make_schedules(current_date, mode)
    database.simulate_observations(current_date, current_date+timedelta(days=7))
    time_since_forecast += timedelta(days=7)
    if time_since_forecast >= limit:
        count, total = database.check_constrained(current_date)
        print('Forecasting on:', current_date)
        database.transit_forecast(current_date, current_date + limit)
        time_since_forecast = timedelta(days=0)

        counts.append(count)
        totals.append(total)
        dates.append(current_date.date())

with open(result_file, 'w') as f:
    f.write('# Date\tNoObserved\tNoTargets')
    for i in range(0,len(totals)):
        f.write('\n'+str(dates[i])+'\t'+str(counts[i])+'\t'+str(totals[i]))

    f.close()

database.store_results(counts[-1], totals[-1])


