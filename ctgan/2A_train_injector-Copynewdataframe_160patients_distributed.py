import os
import socket
import sys

import horovod.keras as hvd
import tensorflow as tf

hostname = socket.gethostname()

hvd.init()
print("Hello from machine {} worker {}, overall rank {}!".format(hostname, hvd.local_rank(), hvd.rank()))

print('pinning process to gpu')
gpus = tf.config.experimental.list_physical_devices('GPU')
print("GPUs on {}: {}".format(hostname, gpus))

for gpu in gpus:
    tf.config.experimental.set_memory_growth(gpu, True)
if gpus:
    tf.config.experimental.set_visible_devices(gpus[hvd.local_rank()], 'GPU')
    print('pinned process to gpu: local rank {}, global rank {}'.format(hvd.local_rank(), hvd.rank()))
else:
    raise IndexError('no GPUs detected!')


from procedures.Training_160patients_distributed import config, Trainer


print("Training CT-GAN Injector on host '{}'...".format(socket.gethostname()))

CTGAN_inj = Trainer(isInjector = False, healthy_samples=config['healthy_samples_test_array'])

number_epochs=100

# read last completed epoch:
skip_epochs = 0
with open(os.path.join(config['progress'], 'completed_epochs'), 'a+') as progress_file:
    try:
        progress_file.seek(0)
        skip_epochs = int(progress_file.read())
    except:
        skip_epochs = 0

if skip_epochs == number_epochs:
    with open(os.path.join(config['progress'], 'completed_epochs'), 'w') as progress_file:
        progress_file.write("0\n")
        skip_epochs = 0

CTGAN_inj.train(epochs=number_epochs, skip_epochs=skip_epochs, batch_size=32, sample_interval=50, skip_batches=0)

print("Done. Ran for {} epochs.".format(number_epochs - skip_epochs))
