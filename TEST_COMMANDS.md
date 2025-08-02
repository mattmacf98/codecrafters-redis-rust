printf "*2\r\n$4\r\nECHO\r\n$5\r\nmango\r\n" | nc localhost 6379

printf "*1\r\n$4\r\nPING\r\n" | nc localhost 6379