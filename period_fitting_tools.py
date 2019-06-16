def read_obs_data(cursor, name):
    cursor.execute('SELECT Epoch, ObsCenter, ObsCenterErr FROM \''+name+'\'')
    rows = cursor.fetchall()
    observations = []
    for row in rows:
        if None not in row:
            #print(row, name)
            new_ob = Observation(row)
            observations.append(new_ob)
    return observations


def period_fit(observations):
    import numpy as np
    epochs = []
    tmids = []
    weights = []
    tmid_max = 0
    tmid_max_err = 0
    for ob in observations:
        epochs.append(ob.epoch)
        tmids.append(ob.tmid)
        try:
            weights.append(1 / ob.tmid_err)
        except ZeroDivisionError:
            weights.append(0)

        if ob.tmid > tmid_max:
            tmid_max = ob.tmid
            tmid_max_err = ob.tmid_err

    try:
        if len(epochs) >= 3:
            poly_both = np.polyfit(epochs, tmids, 1, cov=True, w=weights)
            poly, cov = poly_both[0], poly_both[1]
            fit_period = poly[0]
            fit_period_err = np.sqrt(cov[0][0])

            return fit_period, fit_period_err, tmid_max, tmid_max_err, max(epochs)


    except ValueError:
        pass
    except np.linalg.LinAlgError:
        pass
    except TypeError:
        pass
    raise Warning


def check_better_period(start, fit, fit_err):
    dif = abs(start - fit)
    if fit_err < dif:
        return True
    else:
        return False


class Observation:
    def __init__(self, row):
        self.epoch = row[0]
        self.tmid = row[1]
        self.tmid_err = row[2]
        try:
            self.weight = 1/self.tmid_err
        except ZeroDivisionError:
            self.weight = 0

    def __str__(self):
        return str(self.epoch)+' '+str(self.tmid)