

class BufferedWriter(object):
    def __init__(self, bufsize):
        self.buffer = bytearray(bufsize)

        self.position = 0
        self.buffer_size = bufsize

        super(BufferedWriter, self).__init__()

    def write_into_stream(self):
        raise NotImplementedError

    def write(self, data):
        cdef Py_ssize_t written = 0
        cdef Py_ssize_t to_write
        cdef Py_ssize_t data_len = len(data)

        cdef Py_ssize_t buffer_size = self.buffer_size
        cdef Py_ssize_t pos = self.position

        while written < data_len:
            size = min(data_len - written, buffer_size - pos)
            if size == data_len - written:
                self.buffer[pos:pos + size] = data[written:written + size]
            else:
                self.buffer[pos:pos + size] = data

            if pos == buffer_size:
                self.position = pos
                self.write_into_stream()
                pos = 0

            pos += size
            written += size

        self.buffer_size = buffer_size
        self.position = pos

    def flush(self):
        self.write_into_stream()


class BufferedSocketWriter(BufferedWriter):
    def __init__(self, sock, bufsize):
        self.sock = sock
        super(BufferedSocketWriter, self).__init__(bufsize)

    def write_into_stream(self):
        self.sock.sendall(self.buffer[:self.position])
        self.position = 0


# TODO: make proper CompressedBufferedWriter
