#!/usr/bin/python3
#
# test_encryption.py - Unittest
#
# Frank Blankenburg, Jun. 2017
#

import base64
import os
import random
import unittest

from database.encryption import Encryption

#----------------------------------------------------------------------------
# CLASS TestEncryption
#
class TestEncryption (unittest.TestCase):

    #
    # Generate a random text of random content and length for encryption test
    #
    def generate_random_text (self):

        text = ''
        specials = '{}():,# '

        separator = ''
        for _ in range (random.randint (10, 100)):
            text += separator + base64.urlsafe_b64encode (os.urandom (random.randint (5, 20))).decode ()

            if random.randint (0, 9) < 3:
                text += random.choice (specials)

            separator = ' ' if random.randint (0, 9) > 0 else '\n'

        return text

    #
    # Test if the encrypted text can be decrypted again
    #
    def test_symmetric_encryption (self):
        for _ in range (10):
            encrypt = Encryption ()

            password = encrypt.generate_password ()
            text = self.generate_random_text ()

            encrypted = encrypt.encrypt (text, password)
            decrypted = encrypt.decrypt (encrypted, password)

            self.assertEqual (text, decrypted)
            self.assertNotEqual (text, encrypted)

    #
    # Test if the encrypted text can only be decrypted using the same password
    #
    def test_encryption_safety (self):
        for _ in range (10):
            encrypt = Encryption ()

            password1 = encrypt.generate_password ()
            password2 = encrypt.generate_password ()

            self.assertNotEqual (password1, password2)

            text = self.generate_random_text ()

            encrypted = encrypt.encrypt (text, password1)
            decrypted = encrypt.decrypt (encrypted, password2)

            self.assertNotEqual (text, decrypted)
            self.assertNotEqual (text, encrypted)
