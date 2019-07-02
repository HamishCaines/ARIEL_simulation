class Database:
    def __init__(self, database, telescopes, mode, threshold):
        import sqlite3
        self.db = sqlite3.connect(database)
        self.cursor = self.db.cursor()
        self.names = self.read_target_names()
        self.telescope_file = telescopes
        self.telescope_setup = telescopes.split('/')[-1].split('.')[0]
        self.telescope_data = []
        self.mode = mode
        self.threshold = threshold

    def update(self, table, column, row, value):
        self.cursor.execute('UPDATE '+table+' SET '+column+' = '+str(value)+' WHERE Name = \''+row+'\'')

    def read_target_names(self):
        rows = self.cursor.execute('SELECT Name FROM TARGET_DATA').fetchall()
        names = []
        for row in rows:
            names.append(row[0])
        return names

    def run_queries(self):
        import query_tools
        target_names = self.names
        total = len(target_names)
        count = 1
        print('Starting queries for '+str(len(target_names))+'...')
        for name in target_names:
            try:
                float(name[:-1])
                print('Target ' + name + ' is not real (' + str(count) + '/' + str(total) + ')')
            except ValueError:
                print('Querying ' + name + ' ('+str(count)+'/'+str(total)+')')
                target = query_tools.Target(name)
                target.ETD_query()
                if list(target.__dict__.values()).__contains__(None):
                    target.EXO_query()
                    target.EXO_used = True
                target.write_query_data(self.cursor, self.db)
            count += 1

        print('Initial data obtained, now fitting periods...')
        self.initial_period_fit()
        self.initial_prop_to_ariel()

    def initial_period_fit(self):
        import data_tools
        total = len(self.names)
        count = 1
        for target in self.names:
            obs = data_tools.read_obs_data(self.cursor, target)
            try:
                print('Fitting ' + target + ' ('+str(count)+'/'+str(total)+')')
                fit_period, fit_period_err, latest_tmid, latest_tmid_err, latest_epoch = data_tools.period_fit(obs)
                self.update('TARGET_DATA', 'FitPeriod', target, fit_period)
                self.update('TARGET_DATA', 'FitPeriodErr', target, fit_period_err)
                self.update('TARGET_DATA', 'CurrentPeriod', target, fit_period)
                self.update('TARGET_DATA', 'CurrentPeriodErr', target, fit_period_err)
                self.update('TARGET_DATA', 'TruePeriod', target, fit_period)
                self.update('TARGET_DATA', 'TruePeriodErr', target, fit_period_err)
                self.update('TARGET_DATA', 'TrueEpoch', target, latest_epoch)
                self.update('TARGET_DATA', 'TrueLastObs', target, latest_tmid)
                self.update('TARGET_DATA', 'TrueLastObsErr', target, latest_tmid_err)

                if self.cursor.execute('SELECT LastObsErr FROM TARGET_DATA WHERE Name = \''+target+'\'').fetchall()[0]\
                        is None:
                    self.update('TARGET_DATA', 'LastObs', target, latest_tmid)
                    self.update('TARGET_DATA', 'LastObsErr', target, latest_tmid_err)
                    self.update('TARGET_DATA', 'LastEpoch', target, latest_epoch)

                elif latest_tmid > self.cursor.execute('SELECT LastObs FROM TARGET_DATA WHERE '
                                                       'Name = \''+target+'\'').fetchall()[0][0]:
                    self.update('TARGET_DATA', 'LastObs', target, latest_tmid)
                    self.update('TARGET_DATA', 'LastObsErr', target, latest_tmid_err)
                    self.update('TARGET_DATA', 'LastEpoch', target, latest_epoch)

            except Warning:
                print('Fit for ' + target + ' failed, has '+str(len(obs))+' observations')
                self.cursor.execute(
                    'UPDATE TARGET_DATA SET CurrentPeriod = PeriodStart, CurrentPeriodErr = PeriodStartErr WHERE '
                    'FitPeriod IS NULL')
                self.cursor.execute(
                    'UPDATE TARGET_DATA SET TruePeriod = PeriodStart, TruePeriodErr = PeriodStartErr WHERE FitPeriod IS'
                    ' NULL')
                self.cursor.execute('UPDATE TARGET_DATA SET TrueEpoch = LastEpoch WHERE TrueEpoch IS NULL')
                self.db.commit()

            finally:
                count += 1

    def initial_prop_to_ariel(self):
        import data_tools

        self.cursor.execute(
            'SELECT Name, CurrentPeriod, CurrentPeriodErr, LastObs, LastObsErr, Duration FROM TARGET_DATA '
            'WHERE CurrentPeriodErr NOT NULL AND LastObsErr NOT NULL')
        rows = self.cursor.fetchall()
        for row in rows:
            name = row[0]
            self.cursor.execute('SELECT * FROM \'' + name + '\'')
            observations = len(self.cursor.fetchall())
            try:
                err_tot, percent, loss = data_tools.prop_forwards(row, observations)
                self.update('TARGET_DATA', 'ErrAtAriel', name, err_tot)
                self.update('TARGET_DATA', 'PercentLoss', name, percent)
                self.update('TARGET_DATA', 'LossAtAriel', name, loss)
                self.update('TARGET_DATA', 'ErrAtArielStart', name, err_tot)
                self.update('TARGET_DATA', 'PercentLossStart', name, percent)
                self.update('TARGET_DATA', 'LossAtArielStart', name, loss)
            except Warning:
                pass

    def transit_forecast(self, start, end):
        import observation_tools
        import julian

        self.cursor.execute(
            'CREATE TABLE IF NOT EXISTS DEEP_TRANSITS(Center DATETIME, Name VARCHAR(20), Ingress DATETIME, Egress DATET'
            'IME, Duration TIME, RA DECIMAL(9,7), Dec DECIMAL(9,7), PercentLoss REAL, Epoch REAL, ErrAtAriel DECIMAL(16'
            ',8))')
        start_jd = julian.to_jd(start, fmt='jd') - 2400000
        rows = self.cursor.execute('SELECT Name FROM TARGET_DATA WHERE Depth > 10.0 AND "' + str(
            start_jd) + '" > LastObs AND (ErrAtAriel*24*60 > ' + str(
            self.threshold) + ' OR ErrAtAriel IS NULL)').fetchall()
        deep_names = []
        for row in rows:
            deep_names.append(row[0])
        self.names = deep_names

        for name in self.names:
            data = observation_tools.read_data(self.cursor, name)
            transits = observation_tools.transit_forecast(data, name, start, end)

            for transit in transits:
                string = 'INSERT INTO DEEP_TRANSITS (Center, Name, Ingress, Egress, Duration, RA, Dec, PercentLoss, ' \
                         'Epoch, ErrAtAriel) VALUES ( \'' + str(transit.center) + '\', \'' + transit.name + '\', \'' \
                         + str(transit.ingress) + '\', \'' + str(transit.egress) + '\', \'' + str(
                            transit.duration) + '\', ' + str(transit.ra) + ', ' + str(transit.dec) + ', '
                if transit.loss is None:
                    string += 'NULL, ' + str(transit.epoch) + ', NULL)'
                else:
                    string += str(transit.loss) + ', ' + str(transit.epoch) + ', ' + str(transit.error) + ')'
                self.cursor.execute(string)

        self.db.commit()

    def load_telescope_data(self):
        import observation_tools
        self.cursor.execute('SELECT * FROM TELESCOPES')
        rows = self.cursor.fetchall()
        telescopes = []
        for row in rows:
            new_telescope = observation_tools.Telescope()
            new_telescope.gen_from_database(row)
            telescopes.append(new_telescope)

        self.telescope_data = telescopes

    def obtain_upcoming_transits(self, start_date, interval):
        import observation_tools
        end_date = start_date + interval
        self.cursor.execute('SELECT * FROM DEEP_TRANSITS WHERE Center BETWEEN "' + str(start_date.date()) + ' 00:00:00"'
                            ' AND "' + str(end_date.date()) + ' 00:00:00"')
        rows = self.cursor.fetchall()
        self.load_telescope_data()
        transits = []
        for row in rows:
            new_transit = observation_tools.Transit()
            new_transit.gen_from_database(row)
            if new_transit.check_visibility_telescopes(self.telescope_data):
                if new_transit.error is None:
                    new_transit.loss = 1000
                    new_transit.error = 1000
                if new_transit.error*24*60 > self.threshold:
                    print(new_transit.name + ' IS needed, current error: ' + str(new_transit.error * 24 * 60))
                    transits.append(new_transit)

        transits.sort(key=lambda x: x.loss, reverse=True)

        return transits

    def make_schedules(self, start_date, interval, mode):
        import numpy as np

        transits = self.obtain_upcoming_transits(start_date, interval)

        for telescope in self.telescope_data:
            matching_transits = []
            for transit in transits:
                if telescope.name in transit.telescope:
                    matching_transits.append(transit)

            if mode == 'unlimited':
                limit = np.inf
            elif 'perweek' in mode:
                limit = int(mode.split('perweek')[0])
            else:
                print('NO VALID MODE SPECIFIED')
                raise IOError

            self.schedule(matching_transits, limit, telescope.name)

    def schedule(self, transits, limit, telescope):
        from datetime import datetime, timedelta
        from sqlite3 import IntegrityError

        new_transits = 0
        while new_transits < limit:
            for transit in transits:
                scheduled = self.cursor.execute('SELECT RunStart, RunEnd FROM "'+telescope+'"').fetchall()
                continuum = timedelta(minutes=45)
                new_start = transit.ingress - continuum
                new_end = transit.egress + continuum
                duration = new_end - new_start
                if len(scheduled) == 0:
                    try:
                        self.cursor.execute(
                            'INSERT INTO ' + telescope + ' VALUES ("' + transit.name + '", ' + str(transit.ra) +
                            ', ' + str(transit.dec) + ', "' + str(transit.center) + '", "' + str(
                                new_start) + '", "' + str(new_end) + '", "' +
                            str(duration) + '", ' + str(transit.epoch) + ')')
                        new_transits += 1

                    except IntegrityError:
                        pass

                else:
                    space = True
                    for single in scheduled:
                        try:
                            start_time = datetime.strptime(single[0], '%Y-%m-%d %H:%M:%S.%f')
                            end_time = datetime.strptime(single[1], '%Y-%m-%d %H:%M:%S.%f')
                        except ValueError:
                            start_time = datetime.strptime(single[0], '%Y-%m-%d %H:%M:%S')
                            end_time = datetime.strptime(single[1], '%Y-%m-%d %H:%M:%S')

                        if start_time < new_start < end_time:
                            space = False
                        if start_time < new_end < end_time:
                            space = False
                        if new_start < start_time and end_time < new_end:
                            space = False

                    if space:
                        try:
                            self.cursor.execute(
                                'INSERT INTO ' + telescope + ' VALUES ("' + transit.name + '", ' + str(
                                    transit.ra) +
                                ', ' + str(transit.dec) + ', "' + str(transit.center) + '", "' + str(
                                    new_start) + '", "' + str(new_end) + '", "' +
                                str(duration) + '", ' + str(transit.epoch) + ')')
                            new_transits += 1
                        except IntegrityError:
                            pass

            self.db.commit()
            break

    def simulate_observations(self, start_date, interval):
        import observation_tools
        end_date = start_date + interval
        for telescope in self.telescope_data:
            self.cursor.execute('SELECT * FROM "' + telescope.name + '" WHERE ObsCenter BETWEEN "' + str(
                start_date.date()) + ' 00:00:00" AND "' + str(end_date.date()) + ' 00:00:00"')
            observations = self.cursor.fetchall()

            for observation in observations:
                success = observation_tools.flip_unfair_coin()
                if success:
                    print('Observed ' + observation[0] + ' from ' + telescope.name + ' at ' + str(observation[3]))
                    self.cursor.execute(
                        'SELECT LastObs, LastObsErr, LastEpoch, CurrentPeriod, TrueLastObs, TruePeriod, TrueEpoch FROM '
                        'TARGET_DATA WHERE Name = "' + observation[0] + '"')
                    data = self.cursor.fetchall()[0]
                    new_epoch = observation[-1]
                    last_tmid, last_tmid_err, last_epoch, period, true_t0, true_period, true_epoch = \
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6]

                    new_tmid, new_tmid_err = observation_tools.generate_results(last_tmid, last_tmid_err, last_epoch,
                                                                                new_epoch, period)
                    if true_t0 is not None:
                        true_center = observation_tools.find_true_t0(true_t0, true_period, true_epoch, new_epoch)
                    else:
                        true_center = 'NULL'
                    self.add_new_observation(observation[0], new_epoch, new_tmid, new_tmid_err,
                                             telescope.name, true_center)
                    self.db.commit()

    def add_new_observation(self, name, epoch, tmid, tmid_err, telescope, true_center):
        import observation_tools
        from sqlite3 import IntegrityError

        ids_raw = self.cursor.execute('SELECT ObID FROM "' + name + '"').fetchall()
        new_id = observation_tools.find_highest_id(ids_raw)
        try:
            self.cursor.execute('INSERT INTO "' + name + '" (ObID, Epoch, ObsCenter, ObsCenterErr, TrueCenter, Source) '
                                                         'VALUES (' + str(new_id) + ', ' + str(epoch) + ', '
                                + str(tmid) + ', ' + str(tmid_err) + ', ' + str(true_center)+', "'+telescope + '")')
            self.cursor.execute('UPDATE TARGET_DATA SET NoOfObs = NoOfObs + 1 WHERE Name = "'+name+'"')
            self.db.commit()
        except IntegrityError:
            pass

        self.recalculate(name)

    def recalculate(self, name):
        import data_tools
        import julian
        import datetime

        observations = data_tools.read_obs_data(self.cursor, name)
        try:
            new_period, new_period_err, tmid_max, tmid_max_err, last_epoch = data_tools.period_fit(observations)
            self.update('TARGET_DATA', 'CurrentPeriod', name, new_period)
            self.update('TARGET_DATA', 'CurrentPeriodErr', name, new_period_err)
            self.update('TARGET_DATA', 'LastObs', name, tmid_max)
            self.update('TARGET_DATA', 'LastObsErr', name, tmid_max_err)
            self.update('TARGET_DATA', 'LastEpoch', name, last_epoch)
            #self.update('TARGET_DATA', 'TrueLastObs', name, true_center)
        except Warning:
            if observations is None:
                print('Fit for '+name+'failed, has no observations')
            else:
                if len(observations) != 1:
                    print('Fit for '+name+' failed, has '+str(len(observations))+' observations')
                else:
                    print('Fit for ' + name + ' failed, has ' + str(len(observations)) + ' observation')
        self.db.commit()

        self.cursor.execute(
            'SELECT Name, CurrentPeriod, CurrentPeriodErr, LastObs, LastObsErr, Duration FROM TARGET_DATA '
            'WHERE CurrentPeriodErr NOT NULL AND LastObsErr NOT NULL AND Name = \'' + name + '\'')
        data = self.cursor.fetchall()
        if len(data) != 0:
            data_row = data[0]
            if data_row[3] < (julian.to_jd(datetime.datetime(year=2030, month=6, day=12, hour=0, minute=0, second=0),
                                           fmt='jd') - 2400000):
                err_tot, percent, loss = data_tools.prop_forwards(data_row, len(observations))
                self.update('TARGET_DATA', 'ErrAtAriel', name, err_tot)
                self.update('TARGET_DATA', 'PercentLoss', name, percent)
                self.update('TARGET_DATA', 'LossAtAriel', name, loss)

    def find_earliest_date(self):
        import datetime
        self.cursor.execute('SELECT Center FROM DEEP_TRANSITS')
        rows = self.cursor.fetchall()
        lowest = min(rows)
        return datetime.datetime.strptime(lowest[0], '%Y-%m-%d %H:%M:%S.%f')

    def store_results(self, count, total):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS RESULTS (Network VARCHAR(25), Mode VARCHAR(25), Accuracy REAL, '
                            'Constrained REAL, Total REAL, Percent REAL)')
        percent = count/total*100
        self.cursor.execute('INSERT INTO RESULTS VALUES ("'+self.telescope_setup+'", "'+self.mode+'", '
                            ''+str(self.threshold)+', '+str(count)+', '+str(total)+', '+str(percent)+')')
        self.db.commit()

    def check_constrained(self, current):
        import julian
        current_jd = julian.to_jd(current, fmt='jd') - 2400000
        total = len(self.cursor.execute(
            'SELECT Name FROM TARGET_DATA WHERE LastObs < ' + str(current_jd) + ' AND Depth > 10.0').fetchall())
        count = len(self.cursor.execute('SELECT Name FROM TARGET_DATA WHERE LastObs < '+str(current_jd)+' '
                                        'AND Depth > 10.0 AND ErrAtAriel*24*60 < '+str(self.threshold)).fetchall())
        print('Constrained '+str(count)+'/'+str(total)+' targets on '+str(current.date()))
        return count, total






