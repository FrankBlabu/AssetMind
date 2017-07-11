#!/usr/bin/python3
#
# generator.py - Data sequence from database content generator
#
# Frank Blankenburg, Jul. 2017
#

import argparse
import math
import numpy as np
import scraper

from core.config import Configuration
from database.database import Database

#----------------------------------------------------------------------------
# CLASS Sequence
#
# Object keeping the data of a single training sequence
#
class Sequence:

    #
    # Setup sequence
    #
    # The sequence data is normalized upon construction
    #
    # @param x Input data as a numpy array with shape (y=batchsize, x=channels)
    # @param y Expected data as a numpy array with shape (y=1, x=channels)
    #
    def __init__ (self, x, y):

        assert isinstance (x, np.ndarray)
        assert isinstance (y, np.ndarray)
        assert x.shape[1] == y.shape[1]
        assert y.shape[0] == 1

        self.x = x
        self.y = y

        n = np.concatenate ((x, y))
        self.norm = np.linalg.norm (n)

        if not math.isclose (self.norm, 0):
            self.x /= self.norm
            self.y /= self.norm

    def __repr__ (self):
        return 'Sequence (x={x}, y={y}, norm={norm})'.format (x=self.x, y=self.y, norm=self.norm)


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
        timestamps = self.create_timestamp_map ()

        self.start = min (timestamps.keys ())
        self.end = max (timestamps.keys ())

        self.channels = []
        for k, v in timestamps.items ():
            self.channels.extend (v)
        self.channels = sorted (list (set (self.channels)))

        timestamps = {k: v for k, v in timestamps.items () if len (v) == len (self.channels)}

        #
        # Compute the time span with complete data which can be used for training
        #
        keys = sorted (timestamps.keys ())

        self.block_end = keys[-1] if keys else None
        self.block_start = self.block_end.copy ()

        if self.block_end:
            previous = self.block_start.copy ()
            previous.advance (step=-Configuration.DATABASE_SAMPLING_STEP)

            while previous in timestamps:
                self.block_start = previous.copy ()
                previous.advance (step=-Configuration.DATABASE_SAMPLING_STEP)

        if self.get_number_of_sequences () < 1:
            raise RuntimeError ('Batchsize too large for available data')

    #
    # Return the channels used by the generator
    #
    def get_channels (self):
        return self.channels

    #
    # Return number of steps in the continuous data interval
    #
    def get_number_of_steps (self):
        return int (math.floor ((self.block_end - self.block_start) / Configuration.DATABASE_SAMPLING_STEP))

    #
    # Return number of available sequences
    #
    # In a first step a sequence is assumed to be any possible successive range of samples where
    # there is at least one additional step following left to be the expected outcome
    #
    def get_number_of_sequences (self):
        return int (math.floor (self.get_number_of_steps () - self.batchsize - 1))

    #
    # Return sequence at the given index
    #
    # @return Single training sequence consisting of a input data / expected data tuple
    #
    def get_sequence (self, index):

        if index >= self.get_number_of_sequences ():
            raise RuntimeError ('Index {index} out of bounds. There are {sequences} valid sequences.'
                                .format (index=index, sequences=self.get_number_of_sequences ()))

        input_data = np.zeros ((self.batchsize, len (self.channels)))
        expected_data = np.zeros ((1, len (self.channels)))

        for x in range (len (self.channels)):
            channel = self.channels[x]

            entries = database.get (channel)

            #
            # Build input data array
            #
            for y in range (self.batchsize):
                assert index + y < len (entries)
                entry = entries[index + y]

                if not isinstance (entry.value, float):
                    raise RuntimeError ('Non numeric data present in channel \'{channel}\' at position {position}'
                                        .format (channel=channel.id, position=index + y))

                input_data[y][x] = entry.value

            #
            # Build expected data array
            #
            assert index + self.batchsize < len (entries)
            entry = entries[index + self.batchsize]

            if not isinstance (entry.value, float):
                raise RuntimeError ('Non numeric data present in channel \'{channel}\' at position {position}'
                                    .format (channel=channel.id, position=index + y))

            expected_data[0][x] = entry.value

        return Sequence (input_data, expected_data)

    #
    # Return all timestamps available in the database together with the ids of the
    # channels contributing to these.
    #
    # Only channels containing float data are considered.
    #
    # @return Map of {timestamp: [channel ids]} format
    #
    def create_timestamp_map (self):

        timestamps = {}

        for channel in database.get_all_channels ():

            if channel.type is float:

                for entry in database.get (channel.id):

                    if entry.timestamp not in timestamps:
                        timestamps[entry.timestamp] = [channel.id]
                    else:
                        timestamps[entry.timestamp].append (channel.id)

        for key in timestamps.keys ():
            timestamps[key].sort ()

        return timestamps


#----------------------------------------------------------------------------
# MAIN
#
if __name__ == '__main__':

    parser = argparse.ArgumentParser ()

    parser.add_argument ('-b', '--batchsize', type=int, default=50, help='Training batchsize / sequence length')
    parser.add_argument ('-p', '--password', type=str, default=None, help='Passwort for database encryption')
    parser.add_argument ('database', type=str, default=':memory:', help='Database file')

    args = parser.parse_args ()

    database = Database (args.database, args.password)
    generator = Generator (database, args.batchsize)

    #
    # Compute the channel which is limiting the number of sequences
    #
    timestamps = generator.create_timestamp_map ()
    limiting_channels = '-'

    if generator.block_start != generator.start:

        t = generator.block_start.copy ()
        assert generator.block_start in timestamps

        channels_block_start = set (timestamps[t])
        t.advance (step=-Configuration.DATABASE_SAMPLING_STEP)

        if t in timestamps:
            limiting_channels = [x for x in channels_block_start if x not in timestamps[t]]
        else:
            limiting_channels = 'all'

    all_channels = [channel.id for channel in database.get_all_channels (active_channels_only=False)]
    inactive_channels = sorted ([channel for channel in all_channels if channel not in generator.channels])

    print ('Summary')
    print ('-------')
    print ('Active channels                 : ', generator.get_channels ())
    print ('Inactive channels               : ', inactive_channels)
    print ('Earliest entry                  : ', generator.start)
    print ('Latest entry                    : ', generator.end)
    print ('Continuous complete block start : ', generator.block_start)
    print ('Continuous complete block end   : ', generator.block_end)
    print ('Number of steps                 : ', generator.get_number_of_steps ())
    print ('Number of sequences             : ', generator.get_number_of_sequences ())
    print ('Limiting channels               : ', limiting_channels)
