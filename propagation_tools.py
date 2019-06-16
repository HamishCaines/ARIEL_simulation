def prop_forwards(row, observations):
    import numpy as np
    import datetime
    import julian
    ariel = datetime.datetime(year=2029, month=6, day=12, hour=0, minute=0, second=0)
    ariel_jd = julian.to_jd(ariel, fmt='jd') - 2400000
    period = float(row[1])
    period_err = float(row[2])
    tmid = row[3]
    tmid_err = row[4]
    duration = float(row[5])
    err_tot = 'NULL'
    current = tmid
    count = 0
    if tmid_err is not None and period_err is not None:
        if current < ariel_jd:
            while current < ariel_jd:
                count += 1
                current += period
                err_tot = np.sqrt(tmid_err * tmid_err + (count - observations) * (
                        count - observations) * period_err * period_err)

            percent = err_tot * 24 * 60 / duration * 100
            if percent >= 100.0:
                loss = True
            else:
                loss = False
        else:
            print('End of sim reached')
            raise Warning
    else:
        percent = 1000
        loss = True

    return err_tot, percent, loss
