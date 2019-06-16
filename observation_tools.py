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
    #TODO: Add Error?
    return new_true_t0
