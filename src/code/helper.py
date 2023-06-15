import zipfile
from zipfile import ZipInfo
import zlib
import time
import binascii
import struct

try:
    import zlib  # We may need its compression method
    crc32 = zlib.crc32
except ImportError:
    zlib = None
    crc32 = binascii.crc32


class MemBuffer(object):
    def __init__(self, queue, block_size=8*1024*1024):
        self._count = 0
        self._part_no = 0
        self._buffer = b''
        self._queue = queue
        self._block_size = block_size

    def tell(self):
        return self._count

    def write(self, buf):
        self._buffer += buf
        self._count += len(buf)

        if len(self._buffer) > self._block_size:
            self.push_buffer()

        return len(buf)

    def flush(self):
        self.flush_buffer()

    def flush_buffer(self):
        if len(self._buffer) > 0:
            self.push_buffer()

    def push_buffer(self):
        self._part_no += 1
        self._queue.put((self._part_no, self._buffer))
        self._buffer = b''


class StreamZipFile(zipfile.ZipFile):
    def write_file(self, zinfo_or_arcname, file_object, compress_type=None):
        if not isinstance(zinfo_or_arcname, ZipInfo):
            zinfo = ZipInfo(filename=zinfo_or_arcname,
                            date_time=time.localtime(time.time())[:6])
            zinfo.compress_type = self.compression
            if zinfo.is_dir():
                zinfo.external_attr = 0o40775 << 16   # drwxrwxr-x
                zinfo.external_attr |= 0x10           # MS-DOS directory flag
            else:
                zinfo.external_attr = 0o600 << 16     # ?rw-------
        else:
            zinfo = zinfo_or_arcname
    
        zinfo.flag_bits |= 0x08  # we will write a data descriptor

        if not self.fp:
            raise RuntimeError(
                "Attempt to write to ZIP archive that was already closed")

        if compress_type is not None:
            zinfo.compress_type = compress_type
        else:
            zinfo.compress_type = zipfile.ZIP_DEFLATED
        
        zinfo.CRC = crc = 0
        zinfo.compress_size = compress_size = 0
        zinfo.file_size =  file_size = 0  # Uncompressed size
        
        zinfo.header_offset = self.fp.tell()  # Start of header bytes
        self._writecheck(zinfo)
        self._didModify = True
        self.fp.write(zinfo.FileHeader())

        if zinfo.compress_type == zipfile.ZIP_DEFLATED:
            cmpr = zlib.compressobj(
                zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, -15)
        else:
            cmpr = None

        while True:
            buf = file_object.read(1024 * 1024 * 1)
            if not buf:
                break
            file_size = file_size + len(buf)
            crc = crc32(buf, crc) & 0xffffffff
            if cmpr:
                buf = cmpr.compress(buf)
                compress_size = compress_size + len(buf)
            self.fp.write(buf)

        if cmpr:
            buf = cmpr.flush()
            compress_size = compress_size + len(buf)
            self.fp.write(buf)
            zinfo.compress_size = compress_size
        else:
            zinfo.compress_size = file_size

        zinfo.CRC = crc
        zinfo.file_size = file_size
        
        # Write updated header info, Write CRC and file sizes after the file data
        fmt = '<LLLL'
        self.fp.write(struct.pack(fmt, zipfile._DD_SIGNATURE, zinfo.CRC,
                                  zinfo.compress_size, zinfo.file_size))

        # Successfully written: Add file to our caches
        self.filelist.append(zinfo)
        self.NameToInfo[zinfo.filename] = zinfo
        
        self.start_dir = self.fp.tell()
