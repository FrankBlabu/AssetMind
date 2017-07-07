#!/usr/bin/python3
#
# train.py - Build and train model for coin price predictions
#
# Frank Blankenburg, Jul. 2017
#
# Resources:
#
# * https://github.com/jaungiers/LSTM-Neural-Network-for-Time-Series-Prediction
# * http://www.jakob-aungiers.com/articles/a/LSTM-Neural-Network-for-Time-Series-Prediction
# * https://www.youtube.com/watch?v=2np77NOdnwk
#

import argparse
import gc
import os
import time

from keras.layers.core import Dense, Activation, Dropout
from keras.layers.recurrent import LSTM
from keras.models import Sequential

from database.database import Database


#
# Build LSTM based model
#
def build_model (layers):
    model = Sequential ()

    model.add (LSTM (input_shape=(layers[1], layers[0]),
                     units=layers[1],
                     return_sequences=True))

    model.add (Dropout (0.2))

    model.add (LSTM (units=layers[2],
                     return_sequences=False))

    model.add (Dropout (0.2))

    model.add (Dense (units=layers[3]))

    model.add (Activation ('linear'))

    model.compile (loss='mse', optimizer='rmsprop')

    return model

#
# Train model with the database data
#
def train_model (database):

    #
    # Prepare sequences
    #
    pass



#----------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    #
    # Hide messy tensorflow and numpy warnings
    #
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
    #warnings.filterwarnings ("ignore")

    #
    # Parse command line arguments
    #
    parser = argparse.ArgumentParser ()
    parser.add_argument ('-e', '--epochs', type=int, default=1, help='Number of training epochs')
    parser.add_argument ('-s', '--sequence', type=int, default=50, help='Training sequence length')
    parser.add_argument ('-p', '--password', type=str, default=None, help='Passwort for database encryption')
    parser.add_argument ('-v', '--verbose',  action='store_true', default=False, help='Verbose output')
    parser.add_argument ('database', type=str, default=':memory:', help='Database file')

    args = parser.parse_args ()

    database = Database (args.database, args.password)

    start = time.time ()
    model = build_model ([1, 50, 100, 1])

    if args.verbose:
        print ('Compilation time: {0:.2f}s'.format (time.time () - start))

    start = time.time ()
    train_model (database)

    if args.verbose:
        print ('Training time: {0:.2f}s'.format (time.time () - start))

    #
    # Tensorflow termination bug workaround
    #
    gc.collect ()
