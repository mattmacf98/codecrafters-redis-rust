[![progress-banner](https://backend.codecrafters.io/progress/redis/73340799-3e71-4024-9dea-8894ecb55e5c)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

# Codecrafters Redis
This is my implementation of the redis challenge for codecrafters in rust.

Current Status: Completed all live modules as of 8/10/2025

#TODO
1. A way to do some of the network test locally for multi client interactions / replica interactions.
2. Seperate master instance from replica instance logic using an interface
3. there are lots of parameters going into instacne and client (can I encapsulate some of these into sub structs?)
4. more logical directory structure (i.e. creating resp responses should be in resp module)
5. rdb logic for parsing is very procedural, think about better structure functionally
