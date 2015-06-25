import subprocess
import threading
from azure.storage import BlobService
import os

class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout):
        def target():
            print 'Thread started'
            self.process = subprocess.Popen(self.cmd, shell=True)
            self.process.communicate()
            print 'Thread finished'

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            print 'Terminating process'
            self.process.terminate()
            thread.join()
        print self.process.returncode

#command = Command("echo 'Process started'; sleep 2; echo 'Process finished'")
#print command.run(timeout=3)
#print command.run(timeout=1)
#
#command = Command('ping www.google.com')
#print command.run(timeout=1)

AZURE_STORAGE_CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']

blob_service = BlobService(connection_string=AZURE_STORAGE_CONNECTION_STRING)

print blob_service.put_block_blob_from_path( 'nexradl2', '201208/20120810/KSRX/NWS_NEXRAD_NXL2SR_KSRX_20120810050000_20120810055959.tar', '/snfs9/q2/levelii_tarfiles/201208/20120810/KSRX/NWS_NEXRAD_NXL2SR_KSRX_20120810050000_20120810055959.tar', max_connections=5,)


blobs = blob_service.list_blobs('nexradl2',maxresults=10)
for blob in blobs:
    print(blob.name)
    print(blob.url)

