import os
import numpy as np

#consider your coordinate system, and x vs y

config = {}

### Data Locations

#path to csv where each row indicates where a healthy sample is (format: filename, x, y, z). 'filename' is the folder containing the dcm files of that scan or the mhd file name, slice is the z axis
config['healthy_coords'] = [None] * 20
for i in range(20):
    config['healthy_coords'][i] = "/home/jmangal1/CT-GAN/coviddata/{}.csv".format(i+1)

for i in range(1,21):
    #path to pickle dump of processed healthy samples for testing.
    idx = 'healthy_samples_test{}'.format(i)
    base = "/home/jmangal1/CT-GAN/coviddata/processedsamples{}.npy"
    config[idx] = base.format(i)

config['healthy_samples_test_array'] = [None] * 20
for i in range(20):
    config['healthy_samples_test_array'][i] = "/home/jmangal1/CT-GAN/coviddata/processedsamples{}.npy".format(i+1)


# the coord system used to note the locations of the evidence ('world' or 'vox'). vox is array index.
config['traindata_coordSystem'] = "vox"

# path to save/load trained models and normalization parameters for injector
config['modelpath_inject'] = os.path.join("/home/samit1/160p/coviddata","MODELL","INJ")

# path to save/load trained models and normalization parameters for remover
config['modelpath_remove'] = os.path.join("/home/samit1/160p/coviddata","MODELL","REM")

# path to save snapshots of training progress
config['progress'] = "/home/samit1/160p/IMAGES160PATIENT"

### CT-GAN Configuration

config['cube_shape'] = np.array([32,32,32]) #z,y,x
#configcovid19newconddataframe['cube_shape'] = np.array([8,64,64])

#config['mask_xlims'] = np.array([6,26])
#config['mask_ylims'] = np.array([6,26])
#config['mask_zlims'] = np.array([6,26])

#If true, the noise touch-up is copied onto the tampered region from a hardcoded coordinate. If false, gaussain interpolated noise is added instead
config['copynoise'] = True

# Make save directories
for dir in ['modelpath_inject', 'modelpath_remove', 'progress']:
    if not os.path.exists(config[dir]):
        os.makedirs(config[dir], exist_ok=True)
