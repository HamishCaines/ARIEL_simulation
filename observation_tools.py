def flip_unfair_coin():
    import random
    chance = 0.6
    return True if random.random() < chance else False
    #return True


def generate_results(last_tmid, last_tmid_err, last_epoch, new_epoch, period):
    from random import gauss
    epoch_dif = new_epoch - last_epoch
    new_tmid_exp = last_tmid + period*epoch_dif
    #if last_tmid_err is not None:
    #    new_tmid = gauss(new_tmid_exp, last_tmid_err)
    #    new_tmid_err = abs(gauss(0, last_tmid_err))
    #else:
    #    new_tmid = new_tmid_exp
    #    new_tmid_err = 'NULL'
    new_tmid = gauss(new_tmid_exp, 0.5/24/60)
    new_tmid_err = abs(gauss(0.5, 0.01)/24/60)
    return new_tmid, new_tmid_err


def find_true_t0(true_t0, true_period, last_true_epoch, epoch):
    epoch_dif = epoch - last_true_epoch
    new_true_t0 = true_t0 + epoch_dif*true_period
    #TODO: Add errors?
    return new_true_t0

class Transit:
    def __init__(self):
        self.center = None
        self.name = None
        self.ingress = None
        self.egress = None
        self.duration = None
        self.ra = None
        self.dec = None
        self.loss = None
        self.telescope = []
        self.epoch = None
        self.error = None

    def gen_from_database(self, row):
        import datetime
        try:
            self.center = datetime.datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            self.center = datetime.datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
        self.name = row[1]
        try:
            self.ingress = datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            self.ingress = datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
        try:
            self.egress = datetime.datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            self.egress = datetime.datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S')
        time_split = row[4].split(':')
        self.duration = datetime.timedelta(hours=float(time_split[0]), minutes=float(time_split[1]), seconds=float(time_split[2]))
        self.ra = row[5]
        self.dec = row[6]
        self.loss = row[7]
        self.epoch = row[8]
        self.error = row[9]

    def check_visibility_telescopes(self, telescopes):
        import mini_staralt
        from datetime import timedelta
        accept = False

        while not accept:
            for telescope in telescopes:
                sun_down = False
                sunset, sunrise = mini_staralt.sun_set_rise(
                    self.center.replace(hour=0, second=0, minute=0, microsecond=0), lon=telescope.lon, lat=telescope.lat, sundown=-12)
                # print(sunset, sunrise, self.center, lon, lat)

                if self.ingress > sunset:
                    if self.egress < sunrise:
                        sun_down = True
                elif sunset > self.center:
                    sunset, sunrise = mini_staralt.sun_set_rise(
                        self.center.replace(hour=0, second=0, minute=0, microsecond=0) - timedelta(days=1),
                        lon=telescope.lon, lat=telescope.lat, sundown=-12)

                    if self.ingress > sunset:

                        if self.egress < sunrise:
                            sun_down = True

                if sun_down:

                        # print(sunset, target_rise, target_set, sunrise)
                    try:
                        target_rise, target_set = mini_staralt.target_rise_set(
                            self.center.replace(hour=0, minute=0, second=0, microsecond=0),
                            ra=self.ra, dec=self.dec, mintargetalt=20, lon=telescope.lon, lat=telescope.lat)
                        if self.name == 'XO-6b':
                            print('YES1')

                        if self.ingress > target_rise:
                            if self.egress < target_set:
                                #print(target_rise, self.ingress, self.egress, target_set, self.name, self.ra, self.dec)
                                self.telescope.append(telescope.name)

                                accept = True

                                #if self.telescope is not None:
                                #    self.telescope = [self.telescope, telescope.name]
                                #elif self.telescope is None:
                                #    self.telescope = telescope.name
                            #print(telescope.name, target_rise, self.ingress, self.egress, target_set, accept, self.name)
                        elif target_rise > self.center:
                            target_rise, target_set = mini_staralt.target_rise_set(
                                self.center.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1),
                                ra=self.ra, dec=self.dec, mintargetalt=20, lon=telescope.lon, lat=telescope.lat)

                            if self.ingress > target_rise:
                                if self.egress < target_set:
                                    self.telescope.append(telescope.name)
                                    if self.name == 'XO-6b':
                                        print('YES2')
                                    accept = True
                            #print(telescope.name, target_rise, self.ingress, self.egress, target_set, accept, self.name)
                    except mini_staralt.NeverVisibleError:
                        # print('no')
                        pass
                    except mini_staralt.AlwaysVisibleError:
                        # print('yes')
                        accept = True
                        self.telescope.append(telescope.name)

            break

        return accept


class Telescope:
    def __init__(self):
        self.name = None
        self.lat = None
        self.lon = None
        self.alt = None
        self.aperture = None

    def __str__(self):
        return self.name+' Lat: '+str(self.lat)+' Lon: '+str(self.lon)

    def gen_from_database(self, row):
        self.name = row[0]
        self.lat = row[1]
        self.lon = row[2]
        self.alt = row[3]
        self.aperture = row[4]

def read_data(cursor, target):
    cursor.execute('SELECT LastObs, Duration, CurrentPeriod, RA, Dec, PercentLoss, LastEpoch, ErrAtAriel FROM TARGET_DATA'
                        ' WHERE Name = \'' + target + '\'')
    data = cursor.fetchall()[0]
    return data


def transit_forecast(data, name, start, end):
    from datetime import datetime, timedelta
    import julian

    current_dt = start
    target_dt = end
    current_ephemeris = julian.from_jd(data[0]+2400000, fmt='jd')
    duration = timedelta(minutes=data[1])
    period = timedelta(days=data[2])
    ra = data[3]
    dec = data[4]
    loss = data[5]
    epoch = data[6]
    error = data[7]

    transits = []
    while current_ephemeris < target_dt:
        if current_ephemeris > current_dt:
            candidate = Transit(current_ephemeris, duration, ra, dec, period, loss, name, epoch, error)
            if candidate.check_visibility_general():
                transits.append(candidate)
        current_ephemeris += period
        epoch += 1

    return transits


class Transit:
    def __init__(self, center, duration, ra, dec, period, percentloss, name, epoch, error):
        self.date = center.replace(hour=0, minute=0, second=0, microsecond=0)
        self.name = name
        self.center = center
        self.ingress = center - duration / 2
        self.egress = center + duration / 2
        self.duration = duration
        self.ra = ra
        self.dec = dec
        self.period = period
        self.loss = percentloss
        self.epoch = epoch
        self.error = error

    def check_visibility_general(self):
        import mini_staralt
        import datetime

        latitudes = [45, 0, -45]
        longitudes = [0, 60, 120, 150, 180, 240, 300]
        accept = False

        while not accept:
            for lat in latitudes:
                for lon in longitudes:
                    sun_down = False
                    sunset, sunrise = mini_staralt.sun_set_rise(
                        self.center.replace(hour=0, second=0, minute=0, microsecond=0), lon=lon, lat=lat, sundown=0)
                    # print(sunset, sunrise, self.center, lon, lat)
                    if self.ingress > sunset:
                        if self.egress < sunrise:
                            sun_down = True
                    elif sunset > self.center:
                        sunset, sunrise = mini_staralt.sun_set_rise(
                            self.center.replace(hour=0, second=0, minute=0, microsecond=0) - datetime.timedelta(
                                days=1),
                            lon=lon, lat=lat, sundown=-12)
                        if self.ingress > sunset:
                            if self.egress < sunrise:
                                sun_down = True

                    # print(sun_down, self.center, sunset, sunrise)

                    if sun_down:
                        try:
                            target_rise, target_set = mini_staralt.target_rise_set(
                                self.center.replace(hour=0, minute=0, second=0, microsecond=0),
                                ra=self.ra, dec=self.dec, mintargetalt=20, lon=lon, lat=lat)
                            if self.ingress > target_rise:
                                if self.egress < target_set:
                                    accept = True

                        except mini_staralt.NeverVisibleError:
                            # print('no')
                            pass
                        except mini_staralt.AlwaysVisibleError:
                            # print('yes')
                            accept = True

                        # print(target_rise, target_set, self.center, lon, lat)

            break
        return accept

