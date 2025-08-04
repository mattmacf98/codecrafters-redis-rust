echo -e '*2\r\n$4\r\nECHO\r\n$5\r\nmango\r\n' | nc localhost 6379

echo -e '*1\r\n$4\r\nPING\r\n' | nc localhost 6379

echo -e '*3\r\n$5\r\nBLPOP\r\n$6\r\nbanana\r\n$1\r\n0\r\n' | nc localhost 6379

echo -e '*3\r\n$5\r\nRPUSH\r\n$6\r\nbanana\r\n$10\r\nstrawberry\r\n' | nc localhost 6379