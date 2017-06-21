#!/usr/bin/python3
#
# encryption.py - Encryption/decryption routines for the database
#
# Frank Blankenburg, Jun. 2017
#

import base64
import cryptography
import os
import random
import unittest

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

#----------------------------------------------------------------------------
# Encryption/decryption
#
class Encryption:

    SALT_SEED = 'jfhjs784Hjlonbc23'

    def encrypt (self, text, password):
        f = cryptography.fernet.Fernet (self.get_key (password))
        return f.encrypt (text.encode ()).decode ()

    def decrypt (self, text, password):
        try:
            f = cryptography.fernet.Fernet (self.get_key (password))
            result = f.decrypt (text.encode ()).decode ()
        except cryptography.fernet.InvalidToken:
            result = None

        return result

    def get_key (self, password):
        kdf = PBKDF2HMAC (algorithm=hashes.SHA256 (),
                          length=32,
                          salt=Encryption.SALT_SEED.encode (),
                          iterations=100000,
                          backend=default_backend ())
        return base64.urlsafe_b64encode (kdf.derive (password.encode ()))


#----------------------------------------------------------------------------
# Unittest
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

            password = base64.urlsafe_b64encode (os.urandom (random.randint (5, 20))).decode ()[:-2]
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

            password1 = base64.urlsafe_b64encode (os.urandom (random.randint (5, 20))).decode ()[:-2]
            password2 = base64.urlsafe_b64encode (os.urandom (random.randint (5, 20))).decode ()[:-2]
            text = self.generate_random_text ()

            encrypted = encrypt.encrypt (text, password1)
            decrypted = encrypt.decrypt (encrypted, password2)

            self.assertNotEqual (text, decrypted)
            self.assertNotEqual (text, encrypted)


if __name__ == '__main__':
    encrypt = Encryption ()
    key = encrypt.get_key ('abcdf')

    print (len (key))
    print (key)

    encoded = encrypt.encrypt ("Dieser Text ist doof !", "abcdef")
    print (encoded)
    print (encrypt.decrypt (encoded, "abcdef"))

    test = TestEncryption ()
    print (test.generate_random_text ())

    print (base64.urlsafe_b64encode (os.urandom (random.randint (5, 20))).decode ()[:-2])
