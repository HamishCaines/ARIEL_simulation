import numpy as np
from os import listdir

thresholds = np.arange(3, 10, 1)
modes = ['unlimited', '1perweek', '2perweek', '3perweek', '4perweek']
networks = listdir('telescopes/')
networks.sort()
networks = networks[1:-1]

networks = [networks[0], networks[-1]]
print(thresholds)
total = len(networks)*len(modes)*len(thresholds)
print(total)
sims = []
for threshold in thresholds:
    for network in networks:
        for mode in modes:
            new_sim = 'python3 ../ARIEL_simulator/ARIEL_simulation/run_sim.py '+str(threshold)+' '+network+' '+mode+';'
            sims.append(new_sim)

counter = 0
file_number = 21
with open('sim_bash'+str(file_number)+'.cmd', 'w') as f:
    while counter < 18:
        f.write(sims[counter]+'\n')
        counter += 1







