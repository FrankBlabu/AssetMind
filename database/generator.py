#!/usr/bin/python3
#
# generator.py - Data sequence from database content generator
#
# Frank Blankenburg, Jul. 2017
#

import argparse
import numpy as np
import scraper

from core.config import Configuration
from database.database import Database
from database.database import Entry
from database.database import Channel

#----------------------------------------------------------------------------
# CLASS Generator
#
class Generator:

    #
    # Setup generator
    #
    # Will prepare the data in the databases channels for as training input
    # for the LSTM
    #
    # @param database  Database to fetch the channels from
    # @param batchsize Size of a training batch
    #
    def __init__ (self, database, batchsize):
        self.database = database
        self.batchsize = batchsize

        #
        # Compute timespan in which training data is available and the channels
        # which are providing a numeric data stream
        #
        self.start = None
        self.end = None
        self.channels = []

        for channel in database.get_all_channels ():

            if channel.type is float:

                for entry in database.get (channel.id):
                    self.start = min (self.start, entry.timestamp) if self.start else entry.timestamp
                    self.end = max (self.end, entry.timestamp) if self.end else entry.timestamp

                self.channels.append (channel)

        self.steps = (self.end - self.start) / Configuration.DATABASE_SAMPLING_STEP

        if self.steps < self.batchsize + 1:
            raise RuntimeError ('Batchsize too large for available data')

    #
    # Return number of available sequences
    #
    # In a first step a sequence is assumed to be any possible successive range of samples where
    # there is at least one additional step following left to be the expected outcome
    #
    def get_number_of_sequences (self):
        return self.steps - 1

    #
    # Return sequence at the given index
    #
    def get_sequence (self, index):

        sequence = np.zeros ((self.batchsize, len (self.channels)))

        x = 0
        for channel in self.channels:

            y = 0
            for entry in database.get (channel.id):

                if not isinstance (entry.value, float):
                    raise RuntimeError ('Non numeric data present in channel \'{channel}\''.format (channel=channel.id))

                sequence[y][x] = entry.value
                y = y + 1

            x = x + 1

        return sequence




#----------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':
    parser = argparse.ArgumentParser ()
    parser.add_argument ('-e', '--epochs', type=int, default=1, help='Number of training epochs')
    parser.add_argument ('-s', '--sequence', type=int, default=50, help='Training sequence length')
    parser.add_argument ('-p', '--password', type=str, default=None, help='Passwort for database encryption')
    parser.add_argument ('-v', '--verbose',  action='store_true', default=False, help='Verbose output')
    parser.add_argument ('database', type=str, default=':memory:', help='Database file')

    args = parser.parse_args ()

    database = Database (args.database, args.password)

    generator = Generator (database,args.sequence)

    print (generator.get_number_of_sequences ())
    print (generator.get_sequence (0))
