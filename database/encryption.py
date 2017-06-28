#!/usr/bin/python3
#
# encryption.py - Encryption/decryption routines for the database
#
# Frank Blankenburg, Jun. 2017
#

import base64
import cryptography
import cryptography.fernet
import os
import random

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

#----------------------------------------------------------------------------
# Encryption/decryption
#
class Encryption:

    SALT_SEED = 'jfhjs784Hjlonbc23'

    #
    # Encrypt text
    #
    def encrypt (self, text, password):
        f = cryptography.fernet.Fernet (self.get_key (password))
        return f.encrypt (text.encode ()).decode ()

    #
    # Decrypt text
    #
    def decrypt (self, text, password):
        try:
            f = cryptography.fernet.Fernet (self.get_key (password))
            result = f.decrypt (text.encode ()).decode ()
        except cryptography.fernet.InvalidToken:
            result = None

        return result

    #
    # Generate encryption/decryption key from password and salt
    #
    def get_key (self, password):
        kdf = PBKDF2HMAC (algorithm=hashes.SHA256 (),
                          length=32,
                          salt=Encryption.SALT_SEED.encode (),
                          iterations=100000,
                          backend=default_backend ())
        return base64.urlsafe_b64encode (kdf.derive (password.encode ()))

    #
    # Generate probably safe password
    #
    def generate_password (self):
        return base64.urlsafe_b64encode (os.urandom (random.randint (12, 22))).decode ()[:-2]
