from django.db import models
from django.core.files.storage import FileSystemStorage
from hdfs import InsecureClient

# Custom file storage backend for saving files to HDFS
class HDFSStorage(FileSystemStorage):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Connect to HDFS
        self.client = InsecureClient("url will come here", user="amey")

    def _save(self, name, content):
        # Save the file to HDFS
        with self.client.write(name, overwrite=True) as writer:
            writer.write(content.read())
        return name

# Django model for storing files in HDFS
class HDFSFile(models.Model):
    name = models.CharField(max_length=100)
    file = models.FileField(upload_to='hdfs_files', storage=HDFSStorage())

    def __str__(self):
        return self.name