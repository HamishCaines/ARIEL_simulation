def parse_arguments():
    import argparse

    parser = argparse.ArgumentParser(description='Run simulation')
    parser.add_argument('threshold', type=int, help='Accuracy threshold')
    parser.add_argument('telescopes', type=str, help='file containing telescope data for simulation')
    parser.add_argument('mode', type=str, help='Operating mode')
    args = parser.parse_args()
    telescope_file = '../telescopes/' + args.telescopes
    mode = args.mode
    threshold = args.threshold
    print('Target threshold: ' + str(threshold))
    print('Operating mode: ' + str(mode))
    print('Obtaining telescopes from "' + telescope_file + '"')
    return threshold, telescope_file, mode


def create_folder(telescope_file, threshold, mode):
    sim_name = telescope_file.split('/')[-1].split('.')[0] + '_' + mode + '_' + str(threshold)
    print('Simulation name: '+sim_name+', folder created')
    return sim_name


def copy_database(sim_name):
    import shutil
    from os import mkdir
    database_name = sim_name+'.db'
    try:
        shutil.rmtree(sim_name, ignore_errors=True)
        mkdir(sim_name)
        shutil.copyfile('clean/clean3.db', sim_name + '/' + database_name)
    except FileExistsError:
        pass

    return database_name


def populate_telescopes(database):
    import database_generator
    telescopes = database_generator.generate_sql_table_from_csv(database.telescope_file, 'TELESCOPES', database.cursor)
    for telescope in telescopes:
        database.cursor.execute('DROP TABLE IF EXISTS ' + telescope[0])
        database.cursor.execute('CREATE TABLE IF NOT EXISTS ' + telescope[0] + '(Target VARCHAR(25), RA DECIMAL(16,8), '
                                'Dec DECIMAL(16,8), ObsCenter DATETIME, RunStart DATETIME, RunEnd DATETIME, RunDuration'
                                ' TIME, Epoch REAL, UNIQUE(RunStart))')
        print(telescope[0])
    print('Network has '+str(len(telescopes))+' telescopes')
    database.db.commit()


def run_simulation(database):
    from datetime import datetime, timedelta
    start_date = database.find_earliest_date()
    end_date = datetime(year=2030, month=6, day=12)

    print('Running simulation from '+str(start_date.date())+' until '+str(end_date))

    time_since_forecast = timedelta(days=0)
    limit = timedelta(days=28)

    current_date = start_date

    totals = []
    counts = []
    dates = []
    interval = timedelta(days=7)
    while current_date < end_date:
        current_date += interval
        database.make_schedules(current_date, interval, database.mode)
        database.simulate_observations(current_date, interval)
        time_since_forecast += interval
        if time_since_forecast >= limit:
            count, total = database.check_constrained(current_date)
            print('Forecasting on:', current_date)
            database.transit_forecast(current_date, current_date + limit)
            time_since_forecast = timedelta(days=0)

            counts.append(count)
            totals.append(total)
            dates.append(current_date.date())

    print('Simulation finished, constrained '+str(counts[-1])+'/'+str(totals[-1])+' targets')
    return counts, totals, dates


def write_count_results(counts, totals, dates, sim_name):
    outfile = sim_name+'.csv'
    with open(outfile, 'w') as f:
        f.write('# Date\tNoObserved\tNoTargets')
        for i in range(0, len(totals)):
            f.write('\n' + str(dates[i]) + '\t' + str(counts[i]) + '\t' + str(totals[i]))
    f.close()


def main():
    from os import chdir
    import actions2
    threshold, telescope_file, mode = parse_arguments()
    sim_name = create_folder(telescope_file, threshold, mode)
    database_name = copy_database(sim_name)
    chdir(sim_name)

    database = actions2.Database(database_name, telescope_file, mode, threshold)
    populate_telescopes(database)

    counts, totals, dates = run_simulation(database)
    write_count_results(counts, totals, dates, sim_name)
    database.store_results(counts[-1], totals[-1])


if __name__ == '__main__':
    main()
