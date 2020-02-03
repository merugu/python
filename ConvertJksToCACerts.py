import logging
import base64
import textwrap
import os
import jks


def get_pem(der_bytes, type):
    pem = "-----BEGIN %s-----\n" % type
    pem += "\r\n".join(textwrap.wrap(base64.b64encode(der_bytes).decode('ascii'), 64))
    pem += "\n-----END %s-----\n" % type
    return pem


os.chdir("/etc/foldercontainingcerts")  # change to your current working directory
keystore = jks.KeyStore.load('your.keystore.jks', 'keystorepassword')
truststore = jks.KeyStore.load('your-truststore.jks', 'truststorepASSWORD')

certificate = open('certificate.pem', 'w+')
for alias, pk in keystore.private_keys.items():
    for c in pk.cert_chain:
        certificate.write(get_pem(c[1], "CERTIFICATE"))
certificate.close()
logging.info('Built certfile at %s' % 'certificate.pem')

key = open('key.pem', 'w+')
for alias, pk in keystore.private_keys.items():
    if pk.algorithm_oid == jks.util.RSA_ENCRYPTION_OID:
        key.write(get_pem(pk.pkey, "RSA PRIVATE KEY"))
    else:
        key.write(get_pem(pk.pkey_pkcs8, "PRIVATE KEY"))
key.close()
logging.info('Built keyfile at %s' % 'key.pem')

caroot = open('caroot.pem', 'w+')
for alias, c in truststore.certs.items():
    if alias in ['a-root', 'b-root', 'c-root']:
        caroot.write(get_pem(c.cert, "CERTIFICATE"))
caroot.close()
logging.info('Built cafile at %s' % 'caroot.pem')
