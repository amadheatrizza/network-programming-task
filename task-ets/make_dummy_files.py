import os

# Buat file 10 MB
with open('file10MB.bin', 'wb') as f:
    f.write(os.urandom(10 * 1024 * 1024))  # 10 MB data random

# Buat file 50 MB
with open('file50MB.bin', 'wb') as f:
    f.write(os.urandom(50 * 1024 * 1024))  # 50 MB data random

# Buat file 100 MB
with open('file100MB.bin', 'wb') as f:
    f.write(os.urandom(100 * 1024 * 1024))  # 100 MB data random