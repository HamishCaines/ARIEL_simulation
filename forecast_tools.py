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

