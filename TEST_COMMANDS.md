echo -e '*2\r\n$4\r\nECHO\r\n$5\r\nmango\r\n' | nc localhost 6379

echo -e '*1\r\n$4\r\nPING\r\n' | nc localhost 6379

# BLPOP

echo -e '*3\r\n$5\r\nBLPOP\r\n$6\r\nbanana\r\n$1\r\n0\r\n' | nc localhost 6379

echo -e '*3\r\n$5\r\nRPUSH\r\n$6\r\nbanana\r\n$10\r\nstrawberry\r\n' | nc localhost 6379

# XREAD with BLOCK
echo -e '*6\r\n$5\r\nXREAD\r\n$5\r\nblock\r\n$4\r\n5000\r\n$7\r\nstreams\r\n$6\r\norange\r\n$3\r\n0-1\r\n' | nc localhost 6379

echo -e '*5\r\n$4\r\nXADD\r\n$6\r\norange\r\n$3\r\n0-2\r\n$11\r\ntemperature\r\n$2\r\n77\r\n' | nc localhost 6379