import hashlib
import json
import datetime
import string
import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

#"private_key.pem"

class BlockChain(object):

    def __init__(self, previous_hash=None, private_key_file=None, data={}, uuid=""):
        self.chain = []
        self.records = [{"data": data, "uuid": uuid, "Operator": "CreateBlock"}]
        self.private_key = None

        if not previous_hash:
            previous_hash = 'seed:' + random_string(length=59)
        
        if private_key_file:
            with open(private_key_file, "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=None,
                    backend=default_backend()
                )
        self.private_key = private_key

        self.commit_block(previous_hash=previous_hash)

    def commit_block(self, previous_hash=None):
        data_hash = self.hash(json.dumps(self.records))
        block = {
            'block': len(self.chain) + 1,
            'timestamp': datetime.datetime.today().isoformat(),
            'data': self.records,
            'data_hash': data_hash,
            'previous_block_hash': previous_hash or self.hash(self.chain[-1]),
        }
        if self.private_key:
            block['signature'] = self.sign(data_hash)
        self.records = []
        self.chain.append(block)
        return block

    @property
    def last_block(self):
        return self.chain[-1]

    def add_new_event(self, data={}, operator="Unnamed"):
        self.records.append({"data": data, "operator": operator})
        return self.last_block['block'] + 1

    def hash(self, block):
        string_object = json.dumps(block, sort_keys=True)
        block_string = string_object.encode()
        raw_hash = hashlib.sha256(block_string)
        hex_hash = raw_hash.hexdigest()
        return hex_hash

    def sign(self, plain_text=""):
        signature = self.private_key.sign(
            data=plain_text.encode('utf-8'),
            padding=padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            algorithm=hashes.SHA256()
        )
        return signature.hex()


def random_string(length=64, characters=string.ascii_letters + string.digits):
    result_str = ''.join(random_choice(characters) for i in range(length))
    return result_str

def random_int():
    """
    Select a random integer (64bit)
    """
    return bytes_to_int(os.urandom(8))

def random_range(min, max):
    """
    Select a random integer between two numbers
    """
    return (random_int() % ((max + 1) - min)) + min

def bytes_to_int(bytes):
    """
    Helper function, convert set of bytes to an integer
    """
    result = 0
    for b in bytes:
        result = result * 256 + int(b)
    return result

def random_choice(options):
    """
    Select an item from a list of values
    """
    r = random_range(0, len(options) - 1)
    return options[r]

